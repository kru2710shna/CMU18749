"""
GFD: the Global Fault Detector, new in Milestone 2.

What this process does: sits on the sacred/client machine (per the
assignment's Tips & Tricks) and is the one thing every LFD *and* every
client talks to. Each LFD opens exactly one persistent TCP connection to
the GFD at startup and uses it for two purposes, multiplexed over that
same connection:
  1. A periodic GFD_HELLO/GFD_ACK heartbeat, so the GFD can tell an LFD
     (and therefore its whole machine, per the assignment's assumption
     that "the failure of an LFD is synonymous with the failure of the
     entire machine") is still alive - this is symmetric with how an LFD
     heartbeats its own local replica.
  2. MEMBER_ADD / MEMBER_DELETE notices, sent once each time the LFD's
     replica transitions from down to up (add) or up to down (delete).
     MEMBER_ADD carries the replica's publicly-reachable host/port, since
     the GFD needs to relay that on to clients running on a different
     machine.

Per the updated Milestone 2 spec, clients also connect here (once, at
startup) so they can learn the replica membership dynamically instead of
being told a static list on the command line: a client sends one
CLIENT_HELLO, the GFD immediately replies with a MEMBERSHIP snapshot, and
from then on the GFD pushes a fresh MEMBERSHIP snapshot to every connected
client each time membership changes (add or delete) - this is the exact
"GFD-to-client communication" the rubric requires to be printed at both
ends.

The GFD only needs to track membership - which replica_ids are currently
believed healthy, and where each one is reachable - and print that out
per the rubric's exact wording ("GFD: N members: S1, S2, ..."). It does
not talk to the replicas directly at all; LFDs are the only thing
standing between the GFD and the replicas, exactly as pictured in the
assignment's architecture diagram.
"""

import argparse
import threading

import log_utils
import net_utils
import protocol


class GlobalFaultDetector:
    def __init__(self):
        # membership: replica_id -> (public_host, public_port). A plain
        # dict (not a set) both because we need the host/port per replica
        # to hand to clients, and because Python dicts preserve insertion
        # order, so the printed order matches the order replicas joined -
        # what every example in the assignment shows ("GFD: 2 members:
        # S1, S2", then "... S1, S2, S3").
        self.membership = {}
        # Guards membership since multiple LFD connections (one thread
        # each, via net_utils) can send MEMBER_ADD/MEMBER_DELETE concurrently.
        self._membership_lock = threading.Lock()
        # Every currently-connected client's socket, so a membership change
        # can be broadcast to all of them. Guarded by its own lock since
        # clients connect/disconnect from their own per-connection threads.
        self._clients = []
        self._clients_lock = threading.Lock()

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
            names = ", ".join(self.membership.keys())
            log_utils.log("GFD", f"GFD: {count} member{'s' if count != 1 else ''}: {names}", category="membership")

    def _handle_connection(self, conn, addr) -> None:
        """Per-peer loop, shared by LFDs and clients alike: dispatches on
        message type, since either kind of peer can connect to this one
        listening socket. An LFD's connection stays busy (heartbeats,
        membership notices); a client's connection mostly just sits here
        blocked in receive_line() waiting to be pushed a MEMBERSHIP
        broadcast from some other connection's thread - that's fine, since
        net_utils gives every connection its own thread.
        """
        is_client = False
        try:
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
                    elif message["type"] == protocol.CLIENT_HELLO:
                        is_client = True
                        self._handle_client_hello(conn, message)
        except (OSError, ValueError) as error:
            # A connection can die mid-read for reasons that have nothing to
            # do with the peer choosing to disconnect (a dropped/reset
            # network path, an LFD or client process being killed), or
            # deliver a truncated/garbled line that protocol.parse_message()
            # turns into a ValueError rather than a raw IndexError. net_utils
            # gives every connection its own thread, so neither ever took
            # down the GFD or any other connection - but left uncaught,
            # either dumped a raw traceback per drop instead of the clean
            # one-line message every other peer-to-peer error path in this
            # codebase already uses (see the identical fix in
            # server_replica.py).
            log_utils.log("GFD", f"Connection from {addr} dropped: {error}", category="failure")
        finally:
            if is_client:
                with self._clients_lock:
                    if conn in self._clients:
                        self._clients.remove(conn)

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
        changed = False
        with self._membership_lock:
            if replica_id not in self.membership:
                self.membership[replica_id] = (message["public_host"], message["public_port"])
                log_utils.log("GFD", f"Adding server {replica_id} (registered by {lfd_id})...", category="membership")
                self._print_membership()
                changed = True
        if changed:
            self._broadcast_membership()

    def _handle_member_delete(self, message: dict) -> None:
        lfd_id = message["lfd_id"]
        replica_id = message["replica_id"]
        changed = False
        with self._membership_lock:
            if replica_id in self.membership:
                del self.membership[replica_id]
                log_utils.log("GFD", f"Removing server {replica_id} (reported by {lfd_id})...", category="membership")
                self._print_membership()
                changed = True
        if changed:
            self._broadcast_membership()

    def _handle_client_hello(self, conn, message: dict) -> None:
        """Register a client and immediately send it the current membership
        snapshot, so it can bootstrap its replica connections without
        waiting for the next add/delete event."""
        client_id = message["client_id"]
        log_utils.log("GFD", f"Received CLIENT_HELLO from {client_id}; registering for membership updates.", category="membership")
        with self._clients_lock:
            self._clients.append(conn)
        with self._membership_lock:
            members = [(rid, host, port) for rid, (host, port) in self.membership.items()]
        protocol.send_line(conn, protocol.build_membership(members))
        log_utils.log("GFD", f"Sent initial membership snapshot ({len(members)} member(s)) to {client_id}.", category="membership")

    def _broadcast_membership(self) -> None:
        """Push the current membership snapshot to every connected client.
        Called once per membership change (not per heartbeat), each time
        holding a fresh, self-consistent snapshot rather than an
        incremental delta - a client that missed one broadcast still ends
        up correct on the next one."""
        with self._membership_lock:
            members = [(rid, host, port) for rid, (host, port) in self.membership.items()]
        line = protocol.build_membership(members)

        with self._clients_lock:
            clients = list(self._clients)
        for conn in clients:
            try:
                protocol.send_line(conn, line)
            except OSError:
                # The per-connection thread for this client will notice the
                # closed socket on its own next receive_line() and remove
                # it from self._clients; nothing further to do here.
                continue
        if clients:
            log_utils.log("GFD", f"Sent updated membership snapshot ({len(members)} member(s)) to {len(clients)} connected client(s).", category="membership")


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
