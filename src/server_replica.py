"""
Server replica for Milestones 1 through 4.

Without --rm-host, this behaves as in M1-M3: ready from the start, with the
passive role and backup list taken from the command line.

With --rm-host (M4), the replica registers with the RM and follows its views:
- i_am_ready starts at 0. It becomes 1 when the replica installs a checkpoint,
  or when the RM names it the only ready replica (first of its kind).
- A not-ready active replica logs incoming requests without processing or
  replying. Once a checkpoint arrives it replays the logged requests that the
  checkpoint does not already include.
- A ready replica pushes checkpoints over the secondary channel: in passive
  mode the primary sends them periodically to everyone else, and in active
  mode the first ready replica sends them to not-ready replicas until they
  catch up.
- Each client's high_watermark_request_num travels with the state, so a request
  is never applied twice.
"""

import argparse
import threading
import time

import log_utils
import net_utils
import protocol


# Seconds between state-transfer attempts to a not-ready active replica.
_DEFAULT_ACTIVE_TRANSFER_INTERVAL_SEC = 1.0
# A peer's machine can vanish without closing its connection.
_CHECKPOINT_SEND_TIMEOUT_SEC = 3.0
_RM_RETRY_INTERVAL_SEC = 1.0


def parse_backup_endpoints(backups_arg: str):
    """
    Parse:

        S2:host:checkpoint_port,S3:host:checkpoint_port

    Example on one laptop:

        S2:127.0.0.1:5102,S3:127.0.0.1:5103
    """
    backups = []

    for entry in backups_arg.split(","):
        entry = entry.strip()

        if not entry:
            continue

        try:
            replica_id, host, port_str = entry.split(":")
            backups.append((replica_id, host, int(port_str)))
        except ValueError as error:
            raise ValueError(
                f"Invalid backup endpoint {entry!r}. "
                "Expected S2:host:checkpoint_port."
            ) from error

    if not backups:
        raise ValueError("At least one backup endpoint is required.")

    return backups


class ServerReplica:
    def __init__(
        self,
        replica_id: str,
        replication_mode: str = "active",
        role: str = None,
        checkpoint_freq: float = None,
        backup_endpoints=None,
        rm_host: str = None,
        rm_port: int = None,
        state_transfer_delay: float = 0.0,
    ):
        self.replica_id = replica_id
        self.replication_mode = replication_mode

        self.rm_host = rm_host
        self.rm_port = rm_port
        self.managed = rm_host is not None

        # Refuse checkpoints until this time, so a demo can show the
        # not-ready period, which otherwise lasts only milliseconds.
        self._accept_checkpoints_at = time.monotonic() + state_transfer_delay

        # Without an RM, S1 is initially primary and S2/S3 are backups. With
        # one, every replica is a backup until a view says otherwise.
        if self.managed:
            self.role = "backup"
        elif role is None:
            self.role = "primary" if replica_id == "S1" else "backup"
        else:
            self.role = role

        self.my_state = 0
        self.i_am_ready = 0 if self.managed else 1

        # client_id -> highest request_num seen from that client.
        self.high_watermark_request_num = {}

        # Requests received while not ready, replayed after the checkpoint.
        self._pending_requests = []

        # M3 checkpoint number starts at 1 for every passive replica.
        self.checkpoint_num = 1

        self.checkpoint_freq = checkpoint_freq
        self.backup_endpoints = backup_endpoints or []

        # Latest view from the RM.
        self._view_primary = None
        self._view_ready = []
        self._view_members = []

        # Watermarks in the two most recent checkpoints that reached a backup.
        # Clients may forget only what the older one covers, because the newest
        # may still be in flight if this primary crashes.
        self._stable_watermarks = {}
        self._last_checkpoint_watermarks = {}

        self._state_lock = threading.Lock()

        # Keeps one secondary-channel socket per peer.
        self._checkpoint_sockets = {}
        self._checkpoint_sockets_lock = threading.Lock()

        self._rm_sock = None
        self._rm_send_lock = threading.Lock()

        self._shutdown_event = threading.Event()
        self._checkpoint_wake = threading.Event()

    def run(
        self,
        host: str,
        port: int,
        checkpoint_host: str = None,
        checkpoint_port: int = None,
    ) -> None:
        """
        Start the normal client/LFD/peer listener and the role's background work.
        """
        net_utils.start_tcp_server(host, port, self._handle_connection)

        log_utils.log(
            self.replica_id,
            f"Listening for clients/LFD/peers on {host}:{port}",
            category="lifecycle",
        )

        if self.managed:
            log_utils.log(
                self.replica_id,
                f"Replication mode = {self.replication_mode}; i_am_ready = 0",
                category="lifecycle",
            )

            for target in (self._rm_loop, self._checkpoint_sender_loop):
                threading.Thread(target=target, daemon=True).start()

            return

        if self.replication_mode != "passive":
            return

        log_utils.log(
            self.replica_id,
            f"M3 passive role = {self.role}",
            category="lifecycle",
        )

        if self.role == "primary":
            checkpoint_thread = threading.Thread(
                target=self._checkpoint_sender_loop,
                daemon=True,
            )
            checkpoint_thread.start()

        elif self.role == "backup":
            net_utils.start_tcp_server(
                checkpoint_host or host,
                checkpoint_port,
                self._handle_checkpoint_connection,
            )

            log_utils.log(
                self.replica_id,
                f"Listening for checkpoints on "
                f"{checkpoint_host or host}:{checkpoint_port}",
                category="lifecycle",
            )

    def _handle_connection(self, conn, addr) -> None:
        """Handle clients, LFD heartbeats, and M4 peer checkpoints."""
        with conn:
            try:
                while True:
                    line = protocol.receive_line(conn)

                    if not line:
                        break

                    message = protocol.parse_message(line)

                    if message["type"] == protocol.REQUEST:
                        self._handle_request(conn, message)

                    elif message["type"] == protocol.HEARTBEAT:
                        self._handle_heartbeat(conn, message)

                    elif (
                        self.managed
                        and message["type"] == protocol.CHECKPOINT
                    ):
                        self._handle_checkpoint(message)

                    else:
                        raise ValueError(
                            f"Unexpected normal-channel message: "
                            f"{message['type']!r}"
                        )

            except (OSError, ValueError) as error:
                log_utils.log(
                    self.replica_id,
                    f"Connection from {addr} dropped: {error}",
                    category="failure",
                )

    def _handle_request(self, conn, message: dict) -> None:
        """
        Active mode: every ready replica processes and replies.

        Passive mode: the primary processes and replies; a backup ignores it.

        A not-ready active replica logs the request and sends no reply.
        """
        client_id = message["client_id"]
        request_num = message["request_num"]
        request_tuple = (
            f"<{client_id}, {self.replica_id}, {request_num}, request>"
        )

        if (
            self.replication_mode == "passive"
            and self.role != "primary"
        ):
            log_utils.log(
                self.replica_id,
                f"Passive backup ignoring {request_tuple}; "
                "only the primary processes client requests.",
                category="lifecycle",
            )
            return

        op = message["op"]
        amount = int(message["value"])

        if op != "ADD":
            raise ValueError(f"Unsupported op: {op!r}")

        log_utils.log(
            self.replica_id,
            f"Received {request_tuple} ({op} {amount})",
            category="request_reply",
        )

        lost_requests = False

        with self._state_lock:
            if not self.i_am_ready:
                self._log_pending_request(client_id, request_num, amount)
                return

            high_watermark = self.high_watermark_request_num.get(client_id, 0)

            if request_num <= high_watermark:
                log_utils.log(
                    self.replica_id,
                    f"{request_tuple} is a duplicate "
                    f"(high_watermark_request_num[{client_id}] = "
                    f"{high_watermark}); my_state not changed.",
                    category="duplicate",
                )

            elif self._missed_earlier_requests(request_num, high_watermark):
                log_utils.log(
                    self.replica_id,
                    f"{request_tuple} skips past "
                    f"high_watermark_request_num[{client_id}] = "
                    f"{high_watermark}; requests were missed, "
                    "so i_am_ready = 0 until the next checkpoint.",
                    category="failure",
                )
                self.i_am_ready = 0
                self._log_pending_request(client_id, request_num, amount)
                lost_requests = True

            else:
                log_utils.log(
                    self.replica_id,
                    f"my_state = {self.my_state} "
                    f"before processing {request_tuple}",
                    category="state",
                )

                self.my_state += amount
                self.high_watermark_request_num[client_id] = request_num

                log_utils.log(
                    self.replica_id,
                    f"my_state = {self.my_state} "
                    f"after processing {request_tuple}",
                    category="state",
                )

            new_state = self.my_state
            stable_request_num = self._stable_watermarks.get(client_id, 0)

        if lost_requests:
            self._send_rm_status()
            return

        reply_line = protocol.build_reply(
            client_id,
            self.replica_id,
            request_num,
            "STATE",
            new_state,
            stable_request_num,
        )

        protocol.send_line(conn, reply_line)

        log_utils.log(
            self.replica_id,
            f"Sending <{client_id}, {self.replica_id}, "
            f"{request_num}, reply>",
            category="request_reply",
        )

    def _log_pending_request(
        self,
        client_id: str,
        request_num: int,
        amount: int,
    ) -> None:
        """Remember a request for replay. Caller holds _state_lock."""
        self._pending_requests.append((client_id, request_num, amount))

        self.high_watermark_request_num[client_id] = max(
            request_num,
            self.high_watermark_request_num.get(client_id, 0),
        )

        log_utils.log(
            self.replica_id,
            f"i_am_ready = 0: logged <{client_id}, {self.replica_id}, "
            f"{request_num}, request> without processing or replying; "
            f"high_watermark_request_num = {self.high_watermark_request_num}",
            category="lifecycle",
        )

    def _missed_earlier_requests(
        self,
        request_num: int,
        high_watermark: int,
    ) -> bool:
        """
        True if an active replica skipped a request a peer could supply.

        This happens when a client connects just after this replica's
        checkpoint and has already sent a request only to the other replicas.
        """
        return (
            self.managed
            and self.replication_mode == "active"
            and request_num > high_watermark + 1
            and any(
                replica_id != self.replica_id
                for replica_id in self._view_ready
            )
        )

    def _handle_heartbeat(self, conn, message: dict) -> None:
        """Reply to an LFD heartbeat in every milestone and every role."""
        lfd_id = message["lfd_id"]
        heartbeat_count = message["heartbeat_count"]

        log_utils.log(
            self.replica_id,
            f"Received heartbeat #{heartbeat_count} from {lfd_id}",
            category="heartbeat",
        )

        alive_line = protocol.build_alive(
            self.replica_id,
            lfd_id,
            heartbeat_count,
        )

        protocol.send_line(conn, alive_line)

        log_utils.log(
            self.replica_id,
            f"Sent alive reply #{heartbeat_count} to {lfd_id}",
            category="heartbeat",
        )

    def _rm_loop(self) -> None:
        """Keep a connection to the RM and apply every view it sends."""
        while not self._shutdown_event.is_set():
            try:
                sock = net_utils.connect_to_server(self.rm_host, self.rm_port)

            except OSError as error:
                log_utils.log(
                    self.replica_id,
                    f"Could not connect to RM: {error}",
                    category="failure",
                )
                time.sleep(_RM_RETRY_INTERVAL_SEC)
                continue

            log_utils.log(
                self.replica_id,
                f"Registered with RM at {self.rm_host}:{self.rm_port}",
                category="lifecycle",
            )

            # The RM treats every hello as a process with no state, so a
            # replica reconnecting after a network drop must also catch up.
            with self._state_lock:
                self.i_am_ready = 0

            with self._rm_send_lock:
                self._rm_sock = sock

            try:
                with sock:
                    protocol.send_line(
                        sock,
                        protocol.build_replica_hello(self.replica_id),
                    )

                    while True:
                        line = protocol.receive_line(sock)

                        if not line:
                            raise ConnectionError("RM closed the connection.")

                        message = protocol.parse_message(line)

                        if message["type"] == protocol.VIEW:
                            self._apply_view(message)

            except (OSError, ValueError) as error:
                log_utils.log(
                    self.replica_id,
                    f"RM connection lost: {error}",
                    category="failure",
                )

            with self._rm_send_lock:
                self._rm_sock = None

            time.sleep(_RM_RETRY_INTERVAL_SEC)

    def _send_rm_status(self) -> None:
        """Tell the RM this replica's current i_am_ready value."""
        with self._rm_send_lock:
            if self._rm_sock is None:
                return

            try:
                protocol.send_line(
                    self._rm_sock,
                    protocol.build_replica_status(
                        self.replica_id,
                        self.i_am_ready,
                    ),
                )
            except OSError as error:
                log_utils.log(
                    self.replica_id,
                    f"Could not notify RM: {error}",
                    category="failure",
                )

    def _apply_view(self, message: dict) -> None:
        member_ids = {
            replica_id
            for replica_id, _host, _port in message["members"]
        }

        with self._state_lock:
            self._view_primary = message["primary_id"]
            self._view_ready = message["ready"]
            self._view_members = message["members"]

            names = ", ".join(sorted(member_ids)) or "(none)"
            log_utils.log(
                self.replica_id,
                f"View #{message['view_num']}: members = {names}; "
                f"primary = {self._view_primary or '(none)'}; "
                f"ready = {', '.join(self._view_ready) or '(none)'}",
                category="membership",
            )

            # Removal means the RM saw this replica or its machine as failed.
            # It may have missed requests, so it must catch up before serving.
            if self.i_am_ready and self.replica_id not in member_ids:
                self.i_am_ready = 0
                log_utils.log(
                    self.replica_id,
                    "Removed from membership; i_am_ready = 0 until it catches up.",
                    category="failure",
                )

            # Only a view naming this replica as the sole ready member can make
            # it ready. Any other view may predate its latest status report.
            if not self.i_am_ready and self._view_ready == [self.replica_id]:
                self._replay_pending_requests()
                self.i_am_ready = 1
                log_utils.log(
                    self.replica_id,
                    f"i_am_ready = 1 (first replica of its kind); "
                    f"my_state = {self.my_state}",
                    category="lifecycle",
                )

            if self.replication_mode == "passive":
                self._apply_passive_role()

        # A failed peer's old socket must not carry checkpoints to its
        # relaunched process, which will be listening on a new connection.
        with self._checkpoint_sockets_lock:
            for replica_id in set(self._checkpoint_sockets) - member_ids:
                self._close_checkpoint_socket(replica_id)

        self._checkpoint_wake.set()

    def _apply_passive_role(self) -> None:
        """Follow the RM's primary choice. Caller holds _state_lock."""
        should_be_primary = self._view_primary == self.replica_id

        if should_be_primary and self.role != "primary":
            self.role = "primary"
            # The last received number has already been used by the old primary.
            self.checkpoint_num += 1
            log_utils.log(
                self.replica_id,
                f"Promoted to PRIMARY with my_state = {self.my_state}, "
                f"checkpoint_num = {self.checkpoint_num}",
                category="membership",
            )

        elif not should_be_primary and self.role == "primary":
            self.role = "backup"
            log_utils.log(
                self.replica_id,
                "No longer primary; now a backup.",
                category="membership",
            )

    def _replay_pending_requests(self) -> None:
        """Apply logged requests newer than my state. Caller holds _state_lock."""
        for client_id, request_num, amount in self._pending_requests:
            applied = self.high_watermark_request_num.get(client_id, 0)

            if request_num <= applied:
                continue

            self.my_state += amount
            self.high_watermark_request_num[client_id] = request_num

            log_utils.log(
                self.replica_id,
                f"Replayed logged <{client_id}, {self.replica_id}, "
                f"{request_num}, request>: my_state = {self.my_state}",
                category="state",
            )

        self._pending_requests = []

    def _checkpoint_targets(self) -> list:
        """Return the (replica_id, host, port) peers to send a checkpoint to."""
        if not self.managed:
            return self.backup_endpoints if self.role == "primary" else []

        if not self.i_am_ready:
            return []

        if self.replication_mode == "passive":
            if self.role != "primary":
                return []

            return [
                member
                for member in self._view_members
                if member[0] != self.replica_id
            ]

        # Active: the first ready replica in the view transfers state.
        if not self._view_ready or self._view_ready[0] != self.replica_id:
            return []

        return [
            member
            for member in self._view_members
            if member[0] not in self._view_ready
        ]

    def _checkpoint_sender_loop(self) -> None:
        """
        One checkpoint number labels one logical broadcast. The same state goes
        to every target, then checkpoint_num increments once.
        """
        while not self._shutdown_event.is_set():
            self._checkpoint_wake.wait(self.checkpoint_freq)
            self._checkpoint_wake.clear()

            with self._state_lock:
                targets = self._checkpoint_targets()
                state_snapshot = self.my_state
                checkpoint_num = self.checkpoint_num
                watermark_snapshot = dict(self.high_watermark_request_num)

            if not targets:
                continue

            reached_a_peer = False

            for peer_id, peer_host, peer_port in targets:
                if self._send_checkpoint(
                    peer_id,
                    peer_host,
                    peer_port,
                    checkpoint_num,
                    state_snapshot,
                    watermark_snapshot,
                ):
                    reached_a_peer = True

            with self._state_lock:
                self.checkpoint_num += 1

                if reached_a_peer:
                    self._stable_watermarks = self._last_checkpoint_watermarks
                    self._last_checkpoint_watermarks = watermark_snapshot

    def _close_checkpoint_socket(self, replica_id: str) -> None:
        """Caller holds _checkpoint_sockets_lock."""
        sock = self._checkpoint_sockets.pop(replica_id, None)

        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _send_checkpoint(
        self,
        backup_id: str,
        backup_host: str,
        backup_port: int,
        checkpoint_num: int,
        state_snapshot: int,
        watermark_snapshot: dict,
    ) -> bool:
        """
        Send one checkpoint to one peer and return whether it was sent.

        A failed peer never stops this replica or the other peers: its
        connection is dropped and retried at the next checkpoint interval.
        """
        with self._checkpoint_sockets_lock:
            sock = self._checkpoint_sockets.get(backup_id)

            if sock is None:
                try:
                    sock = net_utils.connect_to_server(
                        backup_host,
                        backup_port,
                    )

                    sock.settimeout(_CHECKPOINT_SEND_TIMEOUT_SEC)
                    self._checkpoint_sockets[backup_id] = sock

                    log_utils.log(
                        self.replica_id,
                        f"Opened secondary checkpoint channel to "
                        f"{backup_id} at {backup_host}:{backup_port}",
                        category="lifecycle",
                    )

                except OSError as error:
                    log_utils.log(
                        self.replica_id,
                        f"Checkpoint #{checkpoint_num} to {backup_id} "
                        f"could not connect ({error}); continuing.",
                        category="failure",
                    )
                    return False

            checkpoint_line = protocol.build_checkpoint(
                self.replica_id,
                checkpoint_num,
                state_snapshot,
                watermark_snapshot,
            )

            try:
                protocol.send_line(sock, checkpoint_line)

                log_utils.log(
                    self.replica_id,
                    f"Sending checkpoint #{checkpoint_num} to {backup_id}: "
                    f"my_state={state_snapshot}, "
                    f"checkpoint_num={checkpoint_num}, "
                    f"high_watermark_request_num={watermark_snapshot}",
                    category="state",
                )
                return True

            except OSError as error:
                self._close_checkpoint_socket(backup_id)

                log_utils.log(
                    self.replica_id,
                    f"Checkpoint #{checkpoint_num} to {backup_id} "
                    f"FAILED ({error}); continuing.",
                    category="failure",
                )
                return False

    def _handle_checkpoint_connection(self, conn, addr) -> None:
        """
        M3 backup-only secondary channel.

        Backups never process client requests. They update state only after
        receiving a checkpoint from S1.
        """
        with conn:
            try:
                while True:
                    line = protocol.receive_line(conn)

                    if not line:
                        break

                    message = protocol.parse_message(line)

                    if message["type"] != protocol.CHECKPOINT:
                        raise ValueError(
                            f"Unexpected checkpoint-channel message: "
                            f"{message['type']!r}"
                        )

                    self._handle_checkpoint(message)

            except (OSError, ValueError) as error:
                log_utils.log(
                    self.replica_id,
                    f"Checkpoint connection from {addr} dropped: {error}",
                    category="failure",
                )

    def _checkpoint_rejection_reason(self, message: dict) -> str:
        """Explain why a checkpoint cannot be installed. Caller holds the lock."""
        if not self.managed:
            return None

        if time.monotonic() < self._accept_checkpoints_at:
            return "--state-transfer-delay has not elapsed"

        if self.replication_mode == "passive":
            if self.role == "primary":
                return "this replica is the primary"

            # A not-ready replica may have a higher number from an old role.
            if (
                self.i_am_ready
                and message["checkpoint_num"] < self.checkpoint_num
            ):
                return f"older than checkpoint #{self.checkpoint_num}"

            return None

        if self.i_am_ready:
            return "state transfer already finished"

        # Every logged request must directly follow the checkpoint or be
        # covered by it, or the requests in between would be lost.
        high_watermarks = message["high_watermarks"]
        checked_clients = set()

        for client_id, request_num, _amount in self._pending_requests:
            if client_id in checked_clients:
                continue

            # Requests are logged in order, so this is the client's earliest.
            checked_clients.add(client_id)

            if request_num > high_watermarks.get(client_id, 0) + 1:
                return (
                    f"it stops before logged request {request_num} "
                    f"from {client_id}; waiting for a newer one"
                )

        return None

    def _handle_checkpoint(self, message: dict) -> None:
        """Install a checkpoint from the primary or a state-transfer donor."""
        sender_id = message["primary_id"]
        checkpoint_num = message["checkpoint_num"]

        with self._state_lock:
            rejection = self._checkpoint_rejection_reason(message)

            if rejection is not None:
                log_utils.log(
                    self.replica_id,
                    f"Ignored checkpoint #{checkpoint_num} from {sender_id}: "
                    f"{rejection}.",
                    category="lifecycle",
                )
                return

            self.my_state = message["my_state"]
            self.checkpoint_num = checkpoint_num
            self.high_watermark_request_num = dict(message["high_watermarks"])

            log_utils.log(
                self.replica_id,
                f"Received checkpoint #{checkpoint_num} from {sender_id}: "
                f"my_state={self.my_state}, "
                f"checkpoint_num={checkpoint_num}, "
                f"high_watermark_request_num="
                f"{self.high_watermark_request_num}",
                category="state",
            )

            self._replay_pending_requests()

            became_ready = not self.i_am_ready
            self.i_am_ready = 1

            if became_ready:
                log_utils.log(
                    self.replica_id,
                    f"i_am_ready = 1 after checkpoint; "
                    f"my_state = {self.my_state}",
                    category="lifecycle",
                )

        if became_ready:
            self._send_rm_status()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an M1-M4 stateful server replica."
    )

    parser.add_argument(
        "--replica-id",
        default="S1",
        help="Replica ID, for example S1, S2, or S3.",
    )

    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host/IP for the normal client/LFD listener.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Normal client/LFD port.",
    )

    parser.add_argument(
        "--replication-mode",
        choices=["active", "passive"],
        default="active",
        help="active preserves M1/M2; passive enables M3.",
    )

    parser.add_argument(
        "--role",
        choices=["primary", "backup"],
        default=None,
        help="M3 role. Defaults to S1=primary and S2/S3=backup. "
             "Ignored with --rm-host, where the RM decides.",
    )

    parser.add_argument(
        "--checkpoint-freq",
        type=float,
        default=None,
        help="Seconds between checkpoints. Required in passive mode for an "
             "M3 primary or any M4 replica. In M4 active mode, the interval "
             f"between state-transfer attempts "
             f"(default {_DEFAULT_ACTIVE_TRANSFER_INTERVAL_SEC}).",
    )

    parser.add_argument(
        "--backups",
        default=None,
        help=(
            "M3 primary only: "
            "S2:host:checkpoint_port,S3:host:checkpoint_port"
        ),
    )

    parser.add_argument(
        "--checkpoint-host",
        default="0.0.0.0",
        help="M3 backup only: host/IP for checkpoint listener.",
    )

    parser.add_argument(
        "--checkpoint-port",
        type=int,
        default=None,
        help="M3 backup only: secondary checkpoint listener port.",
    )

    parser.add_argument(
        "--rm-host",
        default=None,
        help="M4: Replication Manager host/IP. Enables RM-managed recovery.",
    )

    parser.add_argument(
        "--rm-port",
        type=int,
        default=None,
        help="M4: Replication Manager port.",
    )

    parser.add_argument(
        "--state-transfer-delay",
        type=float,
        default=0.0,
        help="M4 demo aid: seconds to refuse checkpoints after launch, which "
             "keeps i_am_ready = 0 long enough to watch requests being logged.",
    )

    args = parser.parse_args()

    if (args.rm_host is None) != (args.rm_port is None):
        raise SystemExit("--rm-host and --rm-port must be supplied together.")

    if args.checkpoint_freq is not None and args.checkpoint_freq <= 0:
        raise SystemExit("--checkpoint-freq must be positive.")

    role = args.role

    if role is None:
        role = "primary" if args.replica_id == "S1" else "backup"

    backup_endpoints = []
    checkpoint_freq = args.checkpoint_freq

    if args.rm_host is not None:
        if checkpoint_freq is None:
            if args.replication_mode == "passive":
                raise SystemExit(
                    "Passive replica requires --checkpoint-freq."
                )

            checkpoint_freq = _DEFAULT_ACTIVE_TRANSFER_INTERVAL_SEC

    elif args.replication_mode == "passive":
        if role == "primary":
            if args.checkpoint_freq is None:
                raise SystemExit(
                    "Passive primary requires a positive --checkpoint-freq."
                )

            if args.backups is None:
                raise SystemExit(
                    "Passive primary requires --backups."
                )

            try:
                backup_endpoints = parse_backup_endpoints(args.backups)
            except ValueError as error:
                raise SystemExit(str(error))

        elif role == "backup":
            if args.checkpoint_port is None:
                raise SystemExit(
                    "Passive backup requires --checkpoint-port."
                )

    replica = ServerReplica(
        replica_id=args.replica_id,
        replication_mode=args.replication_mode,
        role=role,
        checkpoint_freq=checkpoint_freq,
        backup_endpoints=backup_endpoints,
        rm_host=args.rm_host,
        rm_port=args.rm_port,
        state_transfer_delay=args.state_transfer_delay,
    )

    replica.run(
        host=args.host,
        port=args.port,
        checkpoint_host=args.checkpoint_host,
        checkpoint_port=args.checkpoint_port,
    )

    try:
        threading.Event().wait()

    except KeyboardInterrupt:
        replica._shutdown_event.set()
        replica._checkpoint_wake.set()

        log_utils.log(
            args.replica_id,
            "Shutting down (Ctrl-C).",
            category="failure",
        )


if __name__ == "__main__":
    main()
