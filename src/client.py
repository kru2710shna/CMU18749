"""
Client process for Milestones 1 through 4.

M1:
    Connect to one replica and send ADD requests.

M2 active replication:
    Connect to every healthy replica, send each request to all of them,
    deliver the first reply, and discard duplicate replies.

M3 passive replication:
    Learn all replicas from the GFD and open connections to them, but send
    requests only to the primary replica (S1 by default). Only S1 replies.

M4 recovery:
    The GFD also reports the primary chosen by the RM. A request is retried
    under the same request_num until it gets a reply, and servers discard
    duplicates. After a failover, a passive client resends the requests that
    no checkpoint is known to cover, so the new primary does not lose them.
"""

import argparse
import collections
import queue
import socket
import threading
import time

import log_utils
import net_utils
import protocol


# After the first reply, how long to keep collecting duplicates. A not-ready
# replica never replies, and it must not hold up the client's next request.
_DUPLICATE_WINDOW_SEC = 0.3


def _prompt_for_add_amount(client_id: str, default_amount: int) -> int:
    """Read one manual ADD amount, or use Enter for the default."""
    while True:
        raw_input_value = input(
            f"[{client_id}] Enter ADD amount "
            f"(default {default_amount}, 'q' to quit): "
        ).strip()

        if raw_input_value.lower() == "q":
            raise KeyboardInterrupt

        if raw_input_value == "":
            return default_amount

        try:
            return int(raw_input_value)
        except ValueError:
            print(
                f"[{client_id}] Invalid input {raw_input_value!r}; "
                "enter an integer or 'q'."
            )


def run_client(
    client_id: str,
    replica_id: str,
    server_host: str,
    server_port: int,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
) -> None:
    """Milestone 1 single-replica client path."""
    try:
        sock = net_utils.connect_to_server(server_host, server_port)
    except OSError as error:
        log_utils.log(
            client_id,
            f"Could not connect to {replica_id}: {error}",
            category="failure",
        )
        return

    log_utils.log(
        client_id,
        f"Connected to {replica_id} at {server_host}:{server_port}",
        category="lifecycle",
    )

    request_num = 1

    with sock:
        while request_count <= 0 or request_num <= request_count:
            current_amount = (
                _prompt_for_add_amount(client_id, add_amount)
                if manual_mode
                else add_amount
            )

            request_line = protocol.build_request(
                client_id,
                replica_id,
                request_num,
                "ADD",
                current_amount,
            )

            try:
                protocol.send_line(sock, request_line)

                log_utils.log(
                    client_id,
                    f"Sent <{client_id}, {replica_id}, "
                    f"{request_num}, request> (ADD {current_amount})",
                    category="request_reply",
                )

                reply_line = protocol.receive_line(sock)

                if not reply_line:
                    raise ConnectionError(
                        f"Connection to {replica_id} closed unexpectedly."
                    )

                reply = protocol.parse_message(reply_line)

            except (OSError, ConnectionError, ValueError) as error:
                log_utils.log(
                    client_id,
                    f"Connection to {replica_id} failed: {error}",
                    category="failure",
                )
                break

            log_utils.log(
                client_id,
                f"Received <{client_id}, {replica_id}, "
                f"{request_num}, reply> "
                f"(my_state={reply['value']})",
                category="request_reply",
            )

            request_num += 1

            if not manual_mode and request_interval_sec > 0:
                time.sleep(request_interval_sec)


def _parse_replicas_arg(replicas_arg: str):
    """
    Parse:

        S1:host1:5000,S2:host2:5000,S3:host3:5000
    """
    replicas = []

    for entry in replicas_arg.split(","):
        entry = entry.strip()

        if not entry:
            continue

        try:
            replica_id, host, port_str = entry.split(":")
            replicas.append((replica_id, host, int(port_str)))
        except ValueError as error:
            raise ValueError(
                f"Invalid replica entry {entry!r}. "
                "Expected S1:host:port."
            ) from error

    if not replicas:
        raise ValueError("--replicas was given but contained no entries.")

    return replicas


class ReplicaSet:
    """
    Thread-safe registry of sockets to replicas.

    In GFD mode, the background GFD listener updates this registry whenever
    membership changes. In static mode, it is populated once from --replicas.

    Each socket has a reader thread that puts (replica_id, reply) on the
    replies queue, or (replica_id, None) when the connection is lost.
    """

    def __init__(self, client_id: str, primary_id: str = None):
        self.client_id = client_id
        self.primary_id = primary_id
        self.replies = queue.Queue()
        self._lock = threading.Lock()

        # replica_id -> [socket, alive]
        self._entries = {}

    def snapshot(self):
        """Return a point-in-time list of (replica_id, socket, alive)."""
        with self._lock:
            return [
                (replica_id, entry[0], entry[1])
                for replica_id, entry in self._entries.items()
            ]

    def mark_dead(self, replica_id: str, sock: socket.socket) -> None:
        """Mark a connection dead, unless a newer one has replaced it."""
        with self._lock:
            entry = self._entries.get(replica_id)

            if entry is not None and entry[0] is sock:
                entry[1] = False

    def any_alive(self) -> bool:
        with self._lock:
            return any(
                alive
                for _sock, alive in self._entries.values()
            )

    def _read_replies(self, replica_id: str, sock: socket.socket) -> None:
        try:
            while True:
                line = protocol.receive_line(sock)

                if not line:
                    break

                message = protocol.parse_message(line)

                if message["type"] == protocol.REPLY:
                    self.replies.put((replica_id, message))

        except (OSError, ValueError):
            pass

        self.mark_dead(replica_id, sock)
        self.replies.put((replica_id, None))

    @staticmethod
    def _close(sock: socket.socket) -> None:
        # shutdown() wakes the reader thread blocked in recv(); close() alone
        # does not.
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        try:
            sock.close()
        except OSError:
            pass

    def sync_membership(self, members, primary_id: str = None) -> None:
        """
        Reconcile client sockets with a full membership snapshot from GFD.

        In passive mode, clients open connections to every replica, but the
        request loop sends traffic only to the primary.
        """
        if primary_id is not None and primary_id != self.primary_id:
            self.primary_id = primary_id

            log_utils.log(
                self.client_id,
                f"Primary is now {primary_id}.",
                category="membership",
            )

        target_ids = {
            replica_id
            for replica_id, _host, _port in members
        }

        with self._lock:
            current_ids = set(self._entries.keys())

            for replica_id in current_ids - target_ids:
                sock, _alive = self._entries.pop(replica_id)
                self._close(sock)

                log_utils.log(
                    self.client_id,
                    f"Replica {replica_id} left membership; "
                    "connection closed.",
                    category="membership",
                )

        for replica_id, host, port in members:
            with self._lock:
                existing = self._entries.get(replica_id)
                already_alive = (
                    existing is not None
                    and existing[1]
                )

            if already_alive:
                continue

            try:
                sock = net_utils.connect_to_server(host, port)

            except OSError as error:
                log_utils.log(
                    self.client_id,
                    f"Could not connect to {replica_id} "
                    f"at {host}:{port}: {error}",
                    category="failure",
                )
                continue

            log_utils.log(
                self.client_id,
                f"Connected to {replica_id} at {host}:{port}",
                category="lifecycle",
            )

            with self._lock:
                old = self._entries.get(replica_id)
                self._entries[replica_id] = [sock, True]

            threading.Thread(
                target=self._read_replies,
                args=(replica_id, sock),
                daemon=True,
            ).start()

            if old is not None:
                self._close(old[0])

    def close_all(self) -> None:
        with self._lock:
            sockets = [
                sock
                for sock, _alive in self._entries.values()
            ]
            self._entries.clear()

        for sock in sockets:
            self._close(sock)


def _collect_active_replies(
    client_id: str,
    replica_set: ReplicaSet,
    request_num: int,
    expected_replicas: list,
    reply_timeout_sec: float,
) -> bool:
    """Deliver the first reply and visibly discard duplicates."""
    waiting = set(expected_replicas)
    delivered = False
    deadline = time.monotonic() + reply_timeout_sec

    while waiting:
        remaining = deadline - time.monotonic()

        if remaining <= 0:
            break

        try:
            replica_id, reply = replica_set.replies.get(timeout=remaining)
        except queue.Empty:
            break

        if reply is None:
            if replica_id in waiting:
                waiting.discard(replica_id)

                log_utils.log(
                    client_id,
                    f"request_num {request_num}: "
                    f"connection to {replica_id} lost.",
                    category="failure",
                )
            continue

        reply_num = reply["request_num"]

        log_utils.log(
            client_id,
            f"Received <{client_id}, {replica_id}, {reply_num}, reply> "
            f"(my_state={reply['value']})",
            category="request_reply",
        )

        if reply_num != request_num:
            log_utils.log(
                client_id,
                f"request_num {reply_num}: "
                f"Discarded duplicate reply from {replica_id}",
                category="duplicate",
            )
            continue

        waiting.discard(replica_id)

        if delivered:
            log_utils.log(
                client_id,
                f"request_num {request_num}: "
                f"Discarded duplicate reply from {replica_id}",
                category="duplicate",
            )

        else:
            delivered = True
            deadline = min(deadline, time.monotonic() + _DUPLICATE_WINDOW_SEC)

    if delivered and waiting:
        log_utils.log(
            client_id,
            f"request_num {request_num}: no reply yet from "
            f"{', '.join(sorted(waiting))}; continuing.",
            category="lifecycle",
        )

    return delivered


def _run_multireplica_request_loop(
    client_id: str,
    replica_set: ReplicaSet,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
) -> None:
    """
    Active replication: send identical requests to every healthy replica,
    deliver the first reply, and visibly discard duplicate replies.
    """
    request_num = 1
    current_amount = None

    while request_count <= 0 or request_num <= request_count:
        if current_amount is None:
            current_amount = (
                _prompt_for_add_amount(client_id, add_amount)
                if manual_mode
                else add_amount
            )

        sent_to = []

        # Send request_num N to every healthy active replica first.
        for replica_id, sock, alive in replica_set.snapshot():
            if not alive:
                continue

            request_line = protocol.build_request(
                client_id,
                replica_id,
                request_num,
                "ADD",
                current_amount,
            )

            try:
                protocol.send_line(sock, request_line)

                log_utils.log(
                    client_id,
                    f"Sent <{client_id}, {replica_id}, "
                    f"{request_num}, request> "
                    f"(ADD {current_amount})",
                    category="request_reply",
                )

                sent_to.append(replica_id)

            except OSError as error:
                replica_set.mark_dead(replica_id, sock)

                log_utils.log(
                    client_id,
                    f"Connection to {replica_id} failed: {error}",
                    category="failure",
                )

        if not sent_to:
            log_utils.log(
                client_id,
                f"request_num {request_num}: "
                "no replica connected; waiting.",
                category="failure",
            )
            time.sleep(0.5)
            continue

        if not _collect_active_replies(
            client_id,
            replica_set,
            request_num,
            sent_to,
            reply_timeout_sec,
        ):
            log_utils.log(
                client_id,
                f"request_num {request_num}: "
                "no replica responded; retrying.",
                category="failure",
            )
            continue

        request_num += 1
        current_amount = None

        if not manual_mode and request_interval_sec > 0:
            time.sleep(request_interval_sec)


def _exchange_with_primary(
    client_id: str,
    replica_set: ReplicaSet,
    primary_id: str,
    sock: socket.socket,
    request_num: int,
    amount: int,
    reply_timeout_sec: float,
) -> dict:
    """Send one request to the primary and wait for its matching reply."""
    protocol.send_line(
        sock,
        protocol.build_request(
            client_id,
            primary_id,
            request_num,
            "ADD",
            amount,
        ),
    )

    log_utils.log(
        client_id,
        f"Sent <{client_id}, {primary_id}, {request_num}, request> "
        f"(ADD {amount})",
        category="request_reply",
    )

    deadline = time.monotonic() + reply_timeout_sec

    while True:
        remaining = deadline - time.monotonic()

        if remaining <= 0:
            raise TimeoutError("no reply before the timeout")

        try:
            replica_id, reply = replica_set.replies.get(timeout=remaining)
        except queue.Empty:
            raise TimeoutError("no reply before the timeout") from None

        if reply is None:
            if replica_id == primary_id:
                raise ConnectionError("connection to primary lost")
            continue

        if replica_id != primary_id or reply["request_num"] != request_num:
            log_utils.log(
                client_id,
                f"request_num {reply['request_num']}: "
                f"Discarded duplicate reply from {replica_id}",
                category="duplicate",
            )
            continue

        log_utils.log(
            client_id,
            f"Received <{client_id}, {primary_id}, {request_num}, reply> "
            f"(my_state={reply['value']})",
            category="request_reply",
        )

        return reply


def _run_passive_request_loop(
    client_id: str,
    replica_set: ReplicaSet,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
) -> None:
    """
    Passive replication.

    The client may have sockets to every replica, but sends requests only to
    the current primary, so only the primary can produce a reply.
    """
    request_num = 1
    current_amount = None
    retry_delay_sec = min(max(request_interval_sec, 0.1), 1.0)

    # (request_num, amount) for requests no checkpoint is known to cover.
    uncheckpointed = collections.deque()
    last_primary = None

    while request_count <= 0 or request_num <= request_count:
        primary_id = replica_set.primary_id
        primary_socket = None

        for replica_id, sock, alive in replica_set.snapshot():
            if replica_id == primary_id and alive:
                primary_socket = sock
                break

        if primary_socket is None:
            log_utils.log(
                client_id,
                f"Primary {primary_id or '(unknown)'} "
                "is not reachable; waiting.",
                category="failure",
            )
            time.sleep(retry_delay_sec)
            continue

        if current_amount is None:
            current_amount = (
                _prompt_for_add_amount(client_id, add_amount)
                if manual_mode
                else add_amount
            )

        try:
            if last_primary not in (None, primary_id) and uncheckpointed:
                log_utils.log(
                    client_id,
                    f"Failover from {last_primary} to {primary_id}: resending "
                    f"{len(uncheckpointed)} request(s) "
                    "that may not be in its last checkpoint.",
                    category="membership",
                )

                for old_num, old_amount in list(uncheckpointed):
                    _exchange_with_primary(
                        client_id,
                        replica_set,
                        primary_id,
                        primary_socket,
                        old_num,
                        old_amount,
                        reply_timeout_sec,
                    )

            last_primary = primary_id

            reply = _exchange_with_primary(
                client_id,
                replica_set,
                primary_id,
                primary_socket,
                request_num,
                current_amount,
                reply_timeout_sec,
            )

        except OSError as error:
            # TimeoutError and ConnectionError are both OSErrors. The same
            # request_num is retried, and the server discards any duplicate.
            log_utils.log(
                client_id,
                f"request_num {request_num}: "
                f"request to primary {primary_id} failed ({error}); "
                "will retry.",
                category="failure",
            )
            time.sleep(retry_delay_sec)
            continue

        uncheckpointed.append((request_num, current_amount))

        while (
            uncheckpointed
            and uncheckpointed[0][0] <= reply["stable_request_num"]
        ):
            uncheckpointed.popleft()

        request_num += 1
        current_amount = None

        if not manual_mode and request_interval_sec > 0:
            time.sleep(request_interval_sec)


def run_client_multireplica(
    client_id: str,
    replicas: list,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
    replication_mode: str = "active",
    primary_id: str = "S1",
) -> None:
    """Run M2 or M3 using a static --replicas list."""
    replica_set = ReplicaSet(client_id, primary_id)
    replica_set.sync_membership(replicas)

    if not replica_set.any_alive():
        log_utils.log(
            client_id,
            "Could not connect to any replica; exiting.",
            category="failure",
        )
        return

    try:
        if replication_mode == "active":
            _run_multireplica_request_loop(
                client_id,
                replica_set,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

        else:
            _run_passive_request_loop(
                client_id,
                replica_set,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

    finally:
        replica_set.close_all()


def _connect_to_gfd_for_client(
    client_id: str,
    gfd_host: str,
    gfd_port: int,
) -> socket.socket:
    """Retry until the client successfully connects to the GFD."""
    while True:
        try:
            sock = net_utils.connect_to_server(
                gfd_host,
                gfd_port,
            )

            log_utils.log(
                client_id,
                f"Connected to GFD at {gfd_host}:{gfd_port}",
                category="lifecycle",
            )

            return sock

        except OSError as error:
            log_utils.log(
                client_id,
                f"Could not connect to GFD: {error}",
                category="failure",
            )
            time.sleep(1.0)


def _register_with_gfd(
    client_id: str,
    gfd_host: str,
    gfd_port: int,
):
    """Receive the initial full GFD membership snapshot."""
    while True:
        gfd_sock = _connect_to_gfd_for_client(
            client_id,
            gfd_host,
            gfd_port,
        )

        try:
            protocol.send_line(
                gfd_sock,
                protocol.build_client_hello(client_id),
            )

            log_utils.log(
                client_id,
                "Sent CLIENT_HELLO to GFD; "
                "waiting for initial membership.",
                category="lifecycle",
            )

            first_line = protocol.receive_line(gfd_sock)

            if not first_line:
                raise ConnectionError(
                    "GFD closed before initial membership."
                )

            first_message = protocol.parse_message(first_line)

            if first_message["type"] != protocol.MEMBERSHIP:
                raise ValueError(
                    "Expected initial MEMBERSHIP message "
                    f"but received {first_message['type']!r}."
                )

            return gfd_sock, first_message

        except (OSError, ConnectionError, ValueError) as error:
            log_utils.log(
                client_id,
                f"GFD registration failed ({error}); retrying.",
                category="failure",
            )

            try:
                gfd_sock.close()
            except OSError:
                pass

            time.sleep(1.0)


def _gfd_listener_thread(
    client_id: str,
    gfd_sock: socket.socket,
    replica_set: ReplicaSet,
) -> None:
    """
    Receive future GFD membership snapshots.

    When a backup fails, this removes only its idle client connection;
    clients continue using the primary's connection without pausing.
    """
    while True:
        try:
            line = protocol.receive_line(gfd_sock)

            if not line:
                log_utils.log(
                    client_id,
                    "GFD connection closed; no more membership updates.",
                    category="failure",
                )
                return

            message = protocol.parse_message(line)

        except (OSError, ValueError) as error:
            log_utils.log(
                client_id,
                f"GFD listener stopped: {error}",
                category="failure",
            )
            return

        if message["type"] == protocol.MEMBERSHIP:
            members = message["members"]

            names = ", ".join(
                replica_id
                for replica_id, _host, _port in members
            ) or "(none)"

            log_utils.log(
                client_id,
                f"Received membership update from GFD: "
                f"{len(members)} member(s): {names}",
                category="membership",
            )

            replica_set.sync_membership(members, message["primary_id"])


def run_client_via_gfd(
    client_id: str,
    gfd_host: str,
    gfd_port: int,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
    replication_mode: str = "active",
    primary_id: str = "S1",
) -> None:
    """Run M2 active or M3 passive mode using dynamic GFD membership."""
    gfd_sock, first_message = _register_with_gfd(
        client_id,
        gfd_host,
        gfd_port,
    )
    members = first_message["members"]

    replica_set = ReplicaSet(client_id, primary_id)
    replica_set.sync_membership(members, first_message["primary_id"])

    names = ", ".join(
        replica_id
        for replica_id, _host, _port in members
    ) or "(none)"

    log_utils.log(
        client_id,
        f"Received initial membership from GFD: "
        f"{len(members)} member(s): {names}",
        category="membership",
    )

    listener = threading.Thread(
        target=_gfd_listener_thread,
        args=(client_id, gfd_sock, replica_set),
        daemon=True,
    )
    listener.start()

    try:
        if replication_mode == "active":
            _run_multireplica_request_loop(
                client_id,
                replica_set,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

        else:
            _run_passive_request_loop(
                client_id,
                replica_set,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

    finally:
        replica_set.close_all()

        try:
            gfd_sock.close()
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an M1-M4 client."
    )

    parser.add_argument(
        "--client-id",
        required=True,
        help="Unique client ID, for example C1.",
    )

    parser.add_argument(
        "--replica-id",
        default="S1",
        help="M1 target replica ID.",
    )

    parser.add_argument(
        "--server-host",
        default="localhost",
        help="M1 target server host/IP.",
    )

    parser.add_argument(
        "--server-port",
        type=int,
        default=5000,
        help="M1 target server port.",
    )

    parser.add_argument(
        "--add-amount",
        type=int,
        default=1,
        help="Amount added by every request.",
    )

    parser.add_argument(
        "--request-interval",
        type=float,
        default=1.0,
        help="Seconds between automatic requests.",
    )

    parser.add_argument(
        "--request-count",
        type=int,
        default=0,
        help="0 means send forever.",
    )

    parser.add_argument(
        "--mode",
        choices=["auto", "manual"],
        default="auto",
    )

    parser.add_argument(
        "--gfd-host",
        default=None,
        help="M2/M3 GFD host/IP.",
    )

    parser.add_argument(
        "--gfd-port",
        type=int,
        default=None,
        help="M2/M3 GFD port.",
    )

    parser.add_argument(
        "--replicas",
        default=None,
        help=(
            "Static M2/M3 alternative: "
            "S1:host:port,S2:host:port,S3:host:port"
        ),
    )

    parser.add_argument(
        "--reply-timeout-ms",
        type=float,
        default=2000.0,
    )

    parser.add_argument(
        "--replication-mode",
        choices=["active", "passive"],
        default="active",
        help="active=M2; passive=M3.",
    )

    parser.add_argument(
        "--primary-id",
        default="S1",
        help="M3 passive primary replica ID. With an RM, the GFD reports the "
             "primary instead.",
    )

    args = parser.parse_args()

    if (
        args.gfd_host is not None
        and args.gfd_port is None
    ) or (
        args.gfd_host is None
        and args.gfd_port is not None
    ):
        raise SystemExit(
            "--gfd-host and --gfd-port must be supplied together."
        )

    if args.reply_timeout_ms <= 0:
        raise SystemExit(
            "--reply-timeout-ms must be positive."
        )

    try:
        # M2/M3 with dynamic GFD membership.
        if args.gfd_host is not None:
            run_client_via_gfd(
                args.client_id,
                args.gfd_host,
                args.gfd_port,
                args.add_amount,
                args.request_interval,
                args.request_count,
                manual_mode=(args.mode == "manual"),
                reply_timeout_sec=args.reply_timeout_ms / 1000.0,
                replication_mode=args.replication_mode,
                primary_id=args.primary_id,
            )

        # M2/M3 static membership alternative.
        elif args.replicas is not None:
            replicas = _parse_replicas_arg(args.replicas)

            run_client_multireplica(
                args.client_id,
                replicas,
                args.add_amount,
                args.request_interval,
                args.request_count,
                manual_mode=(args.mode == "manual"),
                reply_timeout_sec=args.reply_timeout_ms / 1000.0,
                replication_mode=args.replication_mode,
                primary_id=args.primary_id,
            )

        # Existing M1 single-server mode.
        elif args.replication_mode == "active":
            run_client(
                args.client_id,
                args.replica_id,
                args.server_host,
                args.server_port,
                args.add_amount,
                args.request_interval,
                args.request_count,
                manual_mode=(args.mode == "manual"),
            )

        else:
            raise SystemExit(
                "M3 passive mode requires --gfd-host/--gfd-port "
                "or --replicas."
            )

    except KeyboardInterrupt:
        log_utils.log(
            args.client_id,
            "Shutting down (Ctrl-C).",
            category="lifecycle",
        )


if __name__ == "__main__":
    main()