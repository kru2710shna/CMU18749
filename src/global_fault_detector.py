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

Milestone 4 adds an optional RM (--rm-host/--rm-port). The GFD registers with
the RM at startup and reports every membership change to it. Clients are then
told about changes only once the RM answers with a VIEW, so that each update
they receive also names the current primary.

Per the assignment, the failure of an LFD is the failure of its whole machine.
So if an LFD's connection closes, or it sends nothing for --lfd-timeout seconds
(a machine losing power never closes its connections), the GFD removes every
replica that LFD registered.
"""

import argparse
import threading
import time

import log_utils
import net_utils
import protocol


class GlobalFaultDetector:
    def __init__(self, lfd_timeout: float = 5.0):
        self.lfd_timeout = lfd_timeout
        # replica_id -> the LFD connection that registered it.
        self._member_owner = {}
        self._rm_sock = None
        # Latest (members, primary_id) from the RM. Unused without an RM.
        self._view_members = []
        self._view_primary = None
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

    def run(self, host: str, port: int, rm_host: str = None, rm_port: int = None) -> None:
        self._print_membership()

        # Register with the RM before accepting LFDs, so no change goes unreported.
        if rm_host is not None:
            self._connect_to_rm(rm_host, rm_port)
            threading.Thread(target=self._rm_listener, daemon=True).start()
            self._report_membership_to_rm()

        net_utils.start_tcp_server(host, port, self._handle_connection)
        log_utils.log("GFD", f"Listening on {host}:{port}", category="lifecycle")

    def _connect_to_rm(self, rm_host: str, rm_port: int) -> None:
        while True:
            try:
                self._rm_sock = net_utils.connect_to_server(rm_host, rm_port)
                log_utils.log("GFD", f"Registered with RM at {rm_host}:{rm_port}", category="lifecycle")
                return
            except OSError as error:
                log_utils.log("GFD", f"Could not connect to RM: {error}", category="failure")
                time.sleep(1.0)

    def _report_membership_to_rm(self) -> None:
        """Send the RM a full snapshot. Sent while holding the membership lock
        once LFDs are connected, so the RM sees snapshots in the right order."""
        members = [(rid, host, port) for rid, (host, port) in self.membership.items()]
        try:
            protocol.send_line(self._rm_sock, protocol.build_membership(members))
            log_utils.log("GFD", f"Sent membership ({len(members)} member(s)) to RM.", category="membership")
        except OSError as error:
            log_utils.log("GFD", f"Could not notify RM: {error}", category="failure")

    def _rm_listener(self) -> None:
        """Receive each VIEW from the RM and pass it on to the clients."""
        try:
            while True:
                line = protocol.receive_line(self._rm_sock)
                if not line:
                    break
                message = protocol.parse_message(line)
                if message["type"] == protocol.VIEW:
                    with self._clients_lock:
                        self._view_members = message["members"]
                        self._view_primary = message["primary_id"]
                    self._broadcast_membership()
        except (OSError, ValueError) as error:
            log_utils.log("GFD", f"RM connection failed: {error}", category="failure")
        log_utils.log("GFD", "RM connection closed; clients get no more updates.", category="failure")

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
        lfd_id = None
        try:
            with conn:
                while True:
                    line = protocol.receive_line(conn)
                    if not line:
                        break
                    message = protocol.parse_message(line)

                    if lfd_id is None and "lfd_id" in message:
                        lfd_id = message["lfd_id"]
                        conn.settimeout(self.lfd_timeout)
                        log_utils.log("GFD", f"{lfd_id} registered from {addr[0]}.", category="lifecycle")

                    if message["type"] == protocol.GFD_HELLO:
                        self._handle_hello(conn, message)
                    elif message["type"] == protocol.MEMBER_ADD:
                        self._handle_member_add(conn, message)
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
            elif lfd_id is not None:
                self._handle_lfd_lost(conn, lfd_id)

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

    def _handle_lfd_lost(self, conn, lfd_id: str) -> None:
        """Remove the replicas of an LFD that is gone, unless it has already
        reconnected and re-registered them on a newer connection."""
        log_utils.log("GFD", f"Lost {lfd_id}; its machine is presumed failed.", category="failure")
        changed = False
        with self._membership_lock:
            lost = [rid for rid, owner in self._member_owner.items() if owner is conn]
            for replica_id in lost:
                del self._member_owner[replica_id]
                if replica_id in self.membership:
                    del self.membership[replica_id]
                    log_utils.log("GFD", f"Removing server {replica_id} (lost {lfd_id})...", category="membership")
                    self._print_membership()
                    changed = True
            if changed and self._rm_sock is not None:
                self._report_membership_to_rm()
        if changed and self._rm_sock is None:
            self._broadcast_membership()

    def _handle_member_add(self, conn, message: dict) -> None:
        lfd_id = message["lfd_id"]
        replica_id = message["replica_id"]
        changed = False
        with self._membership_lock:
            self._member_owner[replica_id] = conn
            if replica_id not in self.membership:
                self.membership[replica_id] = (message["public_host"], message["public_port"])
                log_utils.log("GFD", f"Adding server {replica_id} (registered by {lfd_id})...", category="membership")
                self._print_membership()
                changed = True
                if self._rm_sock is not None:
                    self._report_membership_to_rm()
        if changed and self._rm_sock is None:
            self._broadcast_membership()

    def _handle_member_delete(self, message: dict) -> None:
        lfd_id = message["lfd_id"]
        replica_id = message["replica_id"]
        changed = False
        with self._membership_lock:
            self._member_owner.pop(replica_id, None)
            if replica_id in self.membership:
                del self.membership[replica_id]
                log_utils.log("GFD", f"Removing server {replica_id} (reported by {lfd_id})...", category="membership")
                self._print_membership()
                changed = True
                if self._rm_sock is not None:
                    self._report_membership_to_rm()
        if changed and self._rm_sock is None:
            self._broadcast_membership()

    def _client_view(self) -> tuple:
        """Return the MEMBERSHIP line for clients and its member count.
        Caller holds _clients_lock."""
        if self._rm_sock is not None:
            return protocol.build_membership(self._view_members, self._view_primary), len(self._view_members)
        with self._membership_lock:
            members = [(rid, host, port) for rid, (host, port) in self.membership.items()]
        return protocol.build_membership(members), len(members)

    def _handle_client_hello(self, conn, message: dict) -> None:
        """Register a client and immediately send it the current membership
        snapshot, so it can bootstrap its replica connections without
        waiting for the next add/delete event."""
        client_id = message["client_id"]
        log_utils.log("GFD", f"Received CLIENT_HELLO from {client_id}; registering for membership updates.", category="membership")
        # Registering and sending under one lock keeps this snapshot from
        # arriving after, and overwriting, a newer broadcast.
        with self._clients_lock:
            self._clients.append(conn)
            line, member_count = self._client_view()
            protocol.send_line(conn, line)
        log_utils.log("GFD", f"Sent initial membership snapshot ({member_count} member(s)) to {client_id}.", category="membership")

    def _broadcast_membership(self) -> None:
        """Push the current membership snapshot to every connected client.
        Called once per membership change (not per heartbeat), each time
        holding a fresh, self-consistent snapshot rather than an
        incremental delta - a client that missed one broadcast still ends
        up correct on the next one."""
        with self._clients_lock:
            line, member_count = self._client_view()
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
            log_utils.log("GFD", f"Sent updated membership snapshot ({member_count} member(s)) to {len(clients)} connected client(s).", category="membership")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Global Fault Detector.")
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to listen on.")
    parser.add_argument("--port", type=int, default=6000, help="Port to listen on.")
    parser.add_argument("--rm-host", default=None, help="Milestone 4: Replication Manager host/IP.")
    parser.add_argument("--rm-port", type=int, default=None, help="Milestone 4: Replication Manager port.")
    parser.add_argument(
        "--lfd-timeout",
        type=float,
        default=5.0,
        help="Seconds of silence before an LFD's machine is presumed failed. "
             "Must be longer than the LFDs' --heartbeat-freq.",
    )
    args = parser.parse_args()

    if (args.rm_host is None) != (args.rm_port is None):
        raise SystemExit("--rm-host and --rm-port must be supplied together.")

    if args.lfd_timeout <= 0:
        raise SystemExit("--lfd-timeout must be positive.")

    gfd = GlobalFaultDetector(args.lfd_timeout)

    # Block the main thread forever; connection-handling happens in the
    # background threads started by net_utils.start_tcp_server.
    try:
        gfd.run(args.host, args.port, args.rm_host, args.rm_port)
        threading.Event().wait()
    except KeyboardInterrupt:
        log_utils.log("GFD", "Shutting down (Ctrl-C).", category="failure")


if __name__ == "__main__":
    main()