"""
RM: the Replication Manager, new in Milestone 4.

The RM sits above the GFD and owns the decisions the fault detectors do not
make: which replicas are ready to serve and, in passive replication, which one
is primary.

Connections:
- The GFD connects here, sends MEMBERSHIP on every membership change, and
  receives each new VIEW so it can tell the clients.
- Every replica connects here, sends REPLICA_HELLO and REPLICA_STATUS, and
  receives each new VIEW.

Rules:
- A replica joining an empty group is the first of its kind and is ready at
  once. Every later replica stays not ready until it reports that it installed
  a checkpoint.
- The primary is the first ready replica to show up. When it leaves, the RM
  elects the earliest-joined ready replica, so a relaunched replica always
  comes back as a backup.
"""

import argparse
import threading

import log_utils
import net_utils
import protocol


class ReplicationManager:
    def __init__(self, replication_mode: str):
        self.replication_mode = replication_mode

        # replica_id -> (host, port), in the order the replicas joined.
        self.membership = {}
        self.member_count = 0
        self.ready = set()
        self.primary_id = None
        self.view_num = 0

        # Held across sends so every peer sees views in the same order.
        self._lock = threading.Lock()
        self._gfd_conn = None
        self._replica_conns = {}

    def run(self, host: str, port: int) -> None:
        net_utils.start_tcp_server(host, port, self._handle_connection)
        log_utils.log("RM", f"Listening on {host}:{port}", category="lifecycle")
        log_utils.log(
            "RM",
            f"Replication mode = {self.replication_mode}",
            category="lifecycle",
        )
        self._print_membership()

    def _print_membership(self) -> None:
        """Print the exact 'RM: N members: ...' text the rubric checks for."""
        if self.member_count == 0:
            log_utils.log("RM", "RM: 0 members", category="membership")
            return

        names = ", ".join(self.membership)
        plural = "s" if self.member_count != 1 else ""
        log_utils.log(
            "RM",
            f"RM: {self.member_count} member{plural}: {names}",
            category="membership",
        )

    def _handle_connection(self, conn, addr) -> None:
        replica_id = None

        try:
            with conn:
                while True:
                    line = protocol.receive_line(conn)

                    if not line:
                        break

                    message = protocol.parse_message(line)

                    if message["type"] == protocol.MEMBERSHIP:
                        self._handle_gfd_membership(conn, message)

                    elif message["type"] == protocol.REPLICA_HELLO:
                        replica_id = message["replica_id"]
                        self._handle_replica_hello(conn, replica_id)

                    elif message["type"] == protocol.REPLICA_STATUS:
                        self._handle_replica_status(message)

        except (OSError, ValueError) as error:
            log_utils.log(
                "RM",
                f"Connection from {addr} dropped: {error}",
                category="failure",
            )

        finally:
            with self._lock:
                if conn is self._gfd_conn:
                    self._gfd_conn = None
                    log_utils.log("RM", "GFD disconnected.", category="failure")

                if self._replica_conns.get(replica_id) is conn:
                    del self._replica_conns[replica_id]

    def _handle_gfd_membership(self, conn, message: dict) -> None:
        with self._lock:
            if self._gfd_conn is not conn:
                self._gfd_conn = conn
                log_utils.log("RM", "GFD registered.", category="lifecycle")

            self.membership = {
                replica_id: (host, port)
                for replica_id, host, port in message["members"]
            }
            self.member_count = len(self.membership)
            self.ready &= set(self.membership)

            self._print_membership()
            self._publish_view()

    def _handle_replica_hello(self, conn, replica_id: str) -> None:
        with self._lock:
            self._replica_conns[replica_id] = conn

            log_utils.log(
                "RM",
                f"Replica {replica_id} registered.",
                category="lifecycle",
            )

            # A hello means a new process, whose state starts empty, even if
            # the LFD has not yet reported that the old process died.
            if replica_id in self.ready and len(self.ready) > 1:
                self.ready.discard(replica_id)
                self._publish_view()
                return

            self._send_view_to(replica_id)

    def _handle_replica_status(self, message: dict) -> None:
        replica_id = message["replica_id"]

        with self._lock:
            if replica_id not in self.membership:
                return

            if message["i_am_ready"]:
                self.ready.add(replica_id)
            else:
                self.ready.discard(replica_id)

            log_utils.log(
                "RM",
                f"{replica_id} reports i_am_ready = {message['i_am_ready']}",
                category="membership",
            )

            self._publish_view()

    def _publish_view(self) -> None:
        """Decide readiness and primary, then tell replicas and the GFD."""
        if self.membership and not self.ready:
            first = next(iter(self.membership))
            self.ready.add(first)
            log_utils.log(
                "RM",
                f"No ready replica to copy state from; "
                f"{first} is the first of its kind.",
                category="membership",
            )

        self._elect_primary()

        self.view_num += 1

        ready_names = ", ".join(
            replica_id
            for replica_id in self.membership
            if replica_id in self.ready
        ) or "(none)"
        log_utils.log(
            "RM",
            f"View #{self.view_num}: "
            f"primary = {self.primary_id or '(none)'}, "
            f"ready = {ready_names}",
            category="membership",
        )

        for replica_id in list(self._replica_conns):
            self._send_view_to(replica_id)

        if self._gfd_conn is not None:
            try:
                protocol.send_line(self._gfd_conn, self._build_view())
            except OSError as error:
                log_utils.log(
                    "RM",
                    f"Could not send view to GFD: {error}",
                    category="failure",
                )

    def _elect_primary(self) -> None:
        if self.replication_mode != "passive":
            return

        if self.primary_id in self.ready:
            return

        previous = self.primary_id

        self.primary_id = next(
            (
                replica_id
                for replica_id in self.membership
                if replica_id in self.ready
            ),
            None,
        )

        if previous is None and self.primary_id is not None:
            log_utils.log(
                "RM",
                f"RM: {self.primary_id} is the primary replica.",
                category="membership",
            )

        elif previous is not None:
            log_utils.log(
                "RM",
                f"RM: primary {previous} is gone; "
                f"elected new primary {self.primary_id or '(none)'}.",
                category="failure",
            )

    def _build_view(self) -> str:
        members = [
            (replica_id, host, port)
            for replica_id, (host, port) in self.membership.items()
        ]
        ready = [
            replica_id
            for replica_id in self.membership
            if replica_id in self.ready
        ]
        return protocol.build_view(
            self.view_num,
            self.primary_id,
            ready,
            members,
        )

    def _send_view_to(self, replica_id: str) -> None:
        try:
            protocol.send_line(
                self._replica_conns[replica_id],
                self._build_view(),
            )
        except OSError:
            # The connection's own thread sees the failure and cleans up.
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Replication Manager."
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to listen on.")
    # Not 7000: macOS AirPlay Receiver normally listens there.
    parser.add_argument("--port", type=int, default=6100, help="Port to listen on.")
    parser.add_argument(
        "--replication-mode",
        choices=["active", "passive"],
        required=True,
        help="passive also makes the RM elect a primary.",
    )
    args = parser.parse_args()

    manager = ReplicationManager(args.replication_mode)
    manager.run(args.host, args.port)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        log_utils.log("RM", "Shutting down (Ctrl-C).", category="failure")


if __name__ == "__main__":
    main()
