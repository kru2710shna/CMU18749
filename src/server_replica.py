"""
Server replica for Milestones 1, 2, and 3.

Default mode: active replication / M1-M2 behavior.
M3 passive mode:
- Only the primary processes client requests and sends replies.
- Backups remain idle for client requests.
- The primary periodically sends CHECKPOINT messages to backups on
  secondary checkpoint channels.
- Backups install the primary's my_state and checkpoint_num.
"""

import argparse
import threading
import time

import log_utils
import net_utils
import protocol


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
    ):
        self.replica_id = replica_id
        self.replication_mode = replication_mode

        # For M3, S1 is initially primary and S2/S3 are backups.
        if role is None:
            self.role = "primary" if replica_id == "S1" else "backup"
        else:
            self.role = role

        self.my_state = 0

        # M3 checkpoint number starts at 1 for every passive replica.
        self.checkpoint_num = 1

        self.checkpoint_freq = checkpoint_freq
        self.backup_endpoints = backup_endpoints or []

        self._state_lock = threading.Lock()

        # Primary keeps one checkpoint socket per backup.
        self._checkpoint_sockets = {}
        self._checkpoint_sockets_lock = threading.Lock()

        self._shutdown_event = threading.Event()

    def run(
        self,
        host: str,
        port: int,
        checkpoint_host: str = None,
        checkpoint_port: int = None,
    ) -> None:
        """
        Start the normal client/LFD listener.

        In M3 passive mode:
        - primary starts a periodic checkpoint sender;
        - backup starts a secondary checkpoint listener.
        """
        net_utils.start_tcp_server(host, port, self._handle_connection)

        log_utils.log(
            self.replica_id,
            f"Listening for clients/LFD on {host}:{port}",
            category="lifecycle",
        )

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
        """Handle client requests and LFD heartbeats on the normal port."""
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
        Active mode: every server processes requests, preserving M1/M2.

        Passive M3 mode:
        - primary processes and replies;
        - backup does nothing and sends no reply.
        """
        client_id = message["client_id"]
        request_num = message["request_num"]

        if (
            self.replication_mode == "passive"
            and self.role != "primary"
        ):
            log_utils.log(
                self.replica_id,
                f"Passive backup ignoring "
                f"<{client_id}, {self.replica_id}, {request_num}, request>; "
                "only the primary processes client requests.",
                category="lifecycle",
            )
            return

        op = message["op"]
        amount = int(message["value"])

        log_utils.log(
            self.replica_id,
            f"Received <{client_id}, {self.replica_id}, "
            f"{request_num}, request> ({op} {amount})",
            category="request_reply",
        )

        with self._state_lock:
            log_utils.log(
                self.replica_id,
                f"my_state = {self.my_state} before processing "
                f"<{client_id}, {self.replica_id}, "
                f"{request_num}, request>",
                category="state",
            )

            if op != "ADD":
                raise ValueError(f"Unsupported op: {op!r}")

            self.my_state += amount
            new_state = self.my_state

            log_utils.log(
                self.replica_id,
                f"my_state = {self.my_state} after processing "
                f"<{client_id}, {self.replica_id}, "
                f"{request_num}, request>",
                category="state",
            )

        reply_line = protocol.build_reply(
            client_id,
            self.replica_id,
            request_num,
            "STATE",
            new_state,
        )

        protocol.send_line(conn, reply_line)

        log_utils.log(
            self.replica_id,
            f"Sending <{client_id}, {self.replica_id}, "
            f"{request_num}, reply>",
            category="request_reply",
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

    def _checkpoint_sender_loop(self) -> None:
        """
        M3 primary-only loop.

        One checkpoint number labels one logical checkpoint broadcast. The
        primary sends the same state/checkpoint_num to S2 and S3, then
        increments checkpoint_num once for the next periodic checkpoint.
        """
        while not self._shutdown_event.wait(self.checkpoint_freq):
            with self._state_lock:
                state_snapshot = self.my_state
                checkpoint_num = self.checkpoint_num

            for backup_id, backup_host, backup_port in self.backup_endpoints:
                self._send_checkpoint(
                    backup_id,
                    backup_host,
                    backup_port,
                    checkpoint_num,
                    state_snapshot,
                )

            with self._state_lock:
                self.checkpoint_num += 1

    def _send_checkpoint(
        self,
        backup_id: str,
        backup_host: str,
        backup_port: int,
        checkpoint_num: int,
        state_snapshot: int,
    ) -> None:
        """
        Send one checkpoint to one backup.

        A failed backup never stops the primary or the other backup:
        its checkpoint connection is dropped and retried at the next
        checkpoint interval.
        """
        with self._checkpoint_sockets_lock:
            sock = self._checkpoint_sockets.get(backup_id)

            if sock is None:
                try:
                    sock = net_utils.connect_to_server(
                        backup_host,
                        backup_port,
                    )

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
                        f"could not connect ({error}); primary continues.",
                        category="failure",
                    )
                    return

            checkpoint_line = protocol.build_checkpoint(
                self.replica_id,
                checkpoint_num,
                state_snapshot,
            )

            try:
                protocol.send_line(sock, checkpoint_line)

                log_utils.log(
                    self.replica_id,
                    f"Sending checkpoint #{checkpoint_num} to {backup_id}: "
                    f"my_state={state_snapshot}",
                    category="state",
                )

            except OSError as error:
                try:
                    sock.close()
                except OSError:
                    pass

                self._checkpoint_sockets.pop(backup_id, None)

                log_utils.log(
                    self.replica_id,
                    f"Checkpoint #{checkpoint_num} to {backup_id} "
                    f"FAILED ({error}); primary continues.",
                    category="failure",
                )

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

    def _handle_checkpoint(self, message: dict) -> None:
        """Install the state snapshot received from the primary."""
        primary_id = message["primary_id"]
        checkpoint_num = message["checkpoint_num"]
        state_snapshot = message["my_state"]

        with self._state_lock:
            self.my_state = state_snapshot
            self.checkpoint_num = checkpoint_num

        log_utils.log(
            self.replica_id,
            f"Received checkpoint #{checkpoint_num} from {primary_id}: "
            f"my_state={state_snapshot}, "
            f"checkpoint_num={checkpoint_num}",
            category="state",
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an M1/M2/M3 stateful server replica."
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
        help="M3 role. Defaults to S1=primary and S2/S3=backup.",
    )

    parser.add_argument(
        "--checkpoint-freq",
        type=float,
        default=None,
        help="M3 primary only: seconds between checkpoints.",
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

    args = parser.parse_args()

    role = args.role

    if role is None:
        role = "primary" if args.replica_id == "S1" else "backup"

    backup_endpoints = []

    if args.replication_mode == "passive":
        if role == "primary":
            if args.checkpoint_freq is None or args.checkpoint_freq <= 0:
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
        checkpoint_freq=args.checkpoint_freq,
        backup_endpoints=backup_endpoints,
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

        log_utils.log(
            args.replica_id,
            "Shutting down (Ctrl-C).",
            category="failure",
        )


if __name__ == "__main__":
    main()