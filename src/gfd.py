"""
GFD: the Global Fault Detector, new in Milestone 2.

What this process does: sits on the sacred/client machine (per the
assignment's Tips & Tricks) and is the one thing every LFD talks to. Each
LFD opens exactly one persistent TCP connection to the GFD at startup and
uses it for two purposes, multiplexed over that same connection:
  1. A periodic GFD_HELLO/GFD_ACK heartbeat, so the GFD can tell an LFD
     (and therefore its whole machine, per the assignment's assumption
     that "the failure of an LFD is synonymous with the failure of the
     entire machine") is still alive - this is symmetric with how an LFD
     heartbeats its own local replica.
  2. MEMBER_ADD / MEMBER_DELETE notices, sent once each time the LFD's
     replica transitions from down to up (add) or up to down (delete).

The GFD only needs to track membership - which replica_ids are currently
believed healthy - and print that out per the rubric's exact wording
("GFD: N members: S1, S2, ..."). It does not talk to the replicas
directly at all; LFDs are the only thing standing between the GFD and the
replicas, exactly as pictured in the assignment's architecture diagram.
"""

import argparse
import threading

import log_utils
import net_utils
import protocol


class GlobalFaultDetector:
    def __init__(self):
        # membership[] and member_count, named to match the assignment's
        # notation exactly. membership is kept as an ordered list (not a
        # set) so the printed order matches the order replicas joined,
        # which is what every example in the assignment shows
        # ("GFD: 2 members: S1, S2", then "... S1, S2, S3").
        self.membership = []
        # Guards membership[] since multiple LFD connections (one thread
        # each, via net_utils) can send MEMBER_ADD/MEMBER_DELETE concurrently.
        self._membership_lock = threading.Lock()

    def run(self, host: str, port: int) -> None:
        net_utils.start_tcp_server(host, port, self._handle_connection)
        log_utils.log("GFD", f"Listening on {host}:{port}", category="lifecycle")
        self._print_membership()

    def _print_membership(self) -> None:
        """Print the exact 'GFD: N members: ...' text the rubric checks for."""
        count = len(self.membership)
        if count == 0:
            log_utils.log("GFD", "GFD: 0 members", category="membership")
        else:
            names = ", ".join(self.membership)
            log_utils.log("GFD", f"GFD: {count} member{'s' if count != 1 else ''}: {names}", category="membership")

    def _handle_connection(self, conn, addr) -> None:
        """Per-LFD loop: this one persistent connection carries both the
        heartbeat traffic and the membership-change notices from this LFD."""
        with conn:
            while True:
                line = protocol.receive_line(conn)
                if not line:
                    break
                message = protocol.parse_message(line)

                if message["type"] == protocol.GFD_HELLO:
                    self._handle_hello(conn, message)
                elif message["type"] == protocol.MEMBER_ADD:
                    self._handle_member_add(message)
                elif message["type"] == protocol.MEMBER_DELETE:
                    self._handle_member_delete(message)

    def _handle_hello(self, conn, message: dict) -> None:
        lfd_id = message["lfd_id"]
        heartbeat_count = message["heartbeat_count"]
        log_utils.log(
            "GFD",
            f"[{heartbeat_count}] GFD receives heartbeat from {lfd_id}",
            category="heartbeat",
        )
        ack_line = protocol.build_gfd_ack(lfd_id, heartbeat_count)
        protocol.send_line(conn, ack_line)
        log_utils.log(
            "GFD",
            f"[{heartbeat_count}] GFD sending heartbeat ACK to {lfd_id}",
            category="heartbeat",
        )

    def _handle_member_add(self, message: dict) -> None:
        lfd_id = message["lfd_id"]
        replica_id = message["replica_id"]
        with self._membership_lock:
            if replica_id not in self.membership:
                self.membership.append(replica_id)
                log_utils.log("GFD", f"Adding server {replica_id} (registered by {lfd_id})...", category="membership")
                self._print_membership()

    def _handle_member_delete(self, message: dict) -> None:
        lfd_id = message["lfd_id"]
        replica_id = message["replica_id"]
        with self._membership_lock:
            if replica_id in self.membership:
                self.membership.remove(replica_id)
                log_utils.log("GFD", f"Removing server {replica_id} (reported by {lfd_id})...", category="membership")
                self._print_membership()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Global Fault Detector.")
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to listen on.")
    parser.add_argument("--port", type=int, default=6000, help="Port to listen on.")
    args = parser.parse_args()

    gfd = GlobalFaultDetector()
    gfd.run(args.host, args.port)

    # Block the main thread forever; connection-handling happens in the
    # background threads started by net_utils.start_tcp_server.
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        log_utils.log("GFD", "Shutting down (Ctrl-C).", category="failure")


if __name__ == "__main__":
    main()
