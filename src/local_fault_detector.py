"""
LFD1: the Local Fault Detector for Milestone 1 (and, additively, Milestone 2).

What this process does: opens one persistent TCP connection to its local
server replica (S1) and heartbeats it in a loop, forever, at heartbeat_freq
(a CLI argument, never hard-coded per the spec). It counts heartbeats sent
(heartbeat_count) and detects a crash by timing out while waiting for the
ALIVE reply.

Milestone 2 additions (both opt-in via --gfd-host/--gfd-port, so running
this file exactly as in Milestone 1 - i.e. omitting those two flags -
behaves identically to before, with zero GFD involvement):
  - At startup, the LFD opens one persistent TCP connection to the GFD and
    registers with it.
  - A second loop, running in its own thread so it doesn't interfere with
    the (unchanged) replica-heartbeat loop below, periodically sends a
    GFD_HELLO heartbeat to the GFD - the same register-then-heartbeat
    pattern as the replica heartbeat, just one level up the hierarchy.
  - Whenever the replica-heartbeat loop sees its replica transition from
    down/unknown to up, it sends one MEMBER_ADD to the GFD; whenever it
    sees a transition from up to down, it sends one MEMBER_DELETE. Both
    share the LFD's single GFD connection with the GFD_HELLO heartbeat
    above (per the assignment: "1 TCP/IP connection to the GFD"), guarded
    by a lock since two threads can now write to that one socket.
"""

import argparse
import socket
import threading
import time

import log_utils
import net_utils
import protocol

# How long to wait for an ALIVE/GFD_ACK reply before declaring a heartbeat
# timed out. Derived from heartbeat_freq so a slower heartbeat schedule also
# gets a proportionally more patient timeout.
_TIMEOUT_MULTIPLIER = 2.0


class GFDLink:
    """Thread-safe wrapper around the LFD's one persistent socket to the GFD.

    Two threads write to this same connection - the GFD-heartbeat loop
    (below) and, on a state transition, the replica-heartbeat loop - so
    every send is serialized behind a lock. Only the heartbeat loop ever
    reads from this socket, so there's no equivalent need to guard reads.
    """

    def __init__(self, lfd_id: str, sock: socket.socket):
        self.lfd_id = lfd_id
        self.sock = sock
        self._lock = threading.Lock()

    def send_line(self, line: str) -> None:
        """Send one line to the GFD, tolerating the GFD being unreachable.

        Per the assignment's own assumptions, the GFD is an acceptable
        single point of failure. If it happens to be down or its connection
        has dropped exactly when we try to notify it (e.g. mid-shutdown, or
        a network blip), that should degrade to "the GFD's view of this
        replica goes briefly stale" - not crash the LFD, whose primary job
        (detecting and reporting on its own local replica) has nothing to
        do with whether the GFD is currently reachable. Called from three
        places in run_local_fault_detector(); handling it once here, rather
        than wrapping each call site, covers all of them.
        """
        with self._lock:
            try:
                protocol.send_line(self.sock, line)
            except OSError as error:
                log_utils.log(self.lfd_id, f"Could not notify GFD ({error}); continuing without it.", category="failure")


def _connect_to_gfd(lfd_id: str, gfd_host: str, gfd_port: int) -> GFDLink:
    """Keep retrying until the GFD connection is up (mirrors the replica's
    own connect-with-retry loop below), since the LFD registers with the
    GFD once at startup per the assignment."""
    while True:
        try:
            sock = net_utils.connect_to_server(gfd_host, gfd_port)
            log_utils.log(lfd_id, f"Connected to GFD at {gfd_host}:{gfd_port}", category="lifecycle")
            return GFDLink(lfd_id, sock)
        except OSError as error:
            log_utils.log(lfd_id, f"Could not connect to GFD: {error}", category="failure")
            time.sleep(1.0)


def run_gfd_heartbeat_loop(lfd_id: str, gfd_link: GFDLink, heartbeat_freq_sec: float) -> None:
    """Heartbeat the GFD forever, exactly like the replica-heartbeat loop
    below heartbeats the replica - just one level up the hierarchy. Runs in
    its own thread so a slow/failed replica never blocks this heartbeat,
    and vice versa.
    """
    timeout_sec = heartbeat_freq_sec * _TIMEOUT_MULTIPLIER
    gfd_link.sock.settimeout(timeout_sec)
    heartbeat_count = 1

    while True:
        hello_line = protocol.build_gfd_hello(lfd_id, heartbeat_count)
        try:
            gfd_link.send_line(hello_line)
            log_utils.log(
                lfd_id,
                f"[{heartbeat_count}] LFD sending heartbeat to GFD",
                category="heartbeat",
            )

            reply_line = protocol.receive_line(gfd_link.sock)
            if not reply_line:
                raise ConnectionError("Connection closed by GFD")

            protocol.parse_message(reply_line)
            log_utils.log(
                lfd_id,
                f"[{heartbeat_count}] LFD received heartbeat ACK from GFD",
                category="heartbeat",
            )
        except (socket.timeout, OSError, ConnectionError) as error:
            # Per the assignment's assumptions, the GFD (and RM above it)
            # are acceptable single points of failure for this milestone;
            # we just log it and keep retrying rather than tearing down the
            # whole LFD process.
            log_utils.log(
                lfd_id,
                f"[{heartbeat_count}] Heartbeat to GFD FAILED/TIMED OUT ({error}).",
                category="failure",
            )

        heartbeat_count += 1
        time.sleep(heartbeat_freq_sec)


def run_local_fault_detector(lfd_id: str, replica_id: str, server_host: str, server_port: int, heartbeat_freq_sec: float, gfd_link: GFDLink = None, replica_public_host: str = None) -> None:
    heartbeat_count = 1
    timeout_sec = heartbeat_freq_sec * _TIMEOUT_MULTIPLIER
    # Tracks whether the GFD currently believes this replica is a member,
    # so we send MEMBER_ADD/MEMBER_DELETE exactly once per transition
    # (not on every single heartbeat) - "after the first successful
    # heartbeat" per the assignment. None when there's no GFD at all
    # (Milestone 1 mode), in which case these notifications are skipped
    # entirely.
    registered_with_gfd = False if gfd_link is not None else None

    while True:
        try:
            sock = net_utils.connect_to_server(server_host, server_port)
            sock.settimeout(timeout_sec)
        except OSError as error:
            log_utils.log(lfd_id, f"Could not connect to {replica_id}: {error}", category="failure")
            if gfd_link is not None and registered_with_gfd:
                gfd_link.send_line(protocol.build_member_delete(lfd_id, replica_id))
                log_utils.log(lfd_id, f"{lfd_id}: delete replica {replica_id}", category="membership")
                registered_with_gfd = False
            time.sleep(heartbeat_freq_sec)
            continue

        log_utils.log(lfd_id, f"Connected to {replica_id} at {server_host}:{server_port}", category="lifecycle")

        # Keep heartbeating over this one connection until it fails (timeout
        # or disconnect), at which point we report the failure and try to
        # reconnect - this is what lets LFD1 detect S1 being killed and later
        # detect it coming back up if it is relaunched.
        with sock:
            while True:
                heartbeat_line = protocol.build_heartbeat(lfd_id, replica_id, heartbeat_count)
                try:
                    protocol.send_line(sock, heartbeat_line)
                    log_utils.log(
                        lfd_id,
                        f"[{heartbeat_count}] Sending heartbeat to {replica_id}",
                        category="heartbeat",
                    )

                    reply_line = protocol.receive_line(sock)
                    if not reply_line:
                        raise ConnectionError("Connection closed by replica")

                    reply = protocol.parse_message(reply_line)
                    log_utils.log(
                        lfd_id,
                        f"[{heartbeat_count}] Received alive reply from {replica_id}",
                        category="heartbeat",
                    )

                    # First successful heartbeat since startup, or since the
                    # replica last failed: tell the GFD this replica just
                    # became (or became again) a healthy member.
                    if gfd_link is not None and not registered_with_gfd:
                        gfd_link.send_line(protocol.build_member_add(lfd_id, replica_id, replica_public_host, server_port))
                        log_utils.log(lfd_id, f"{lfd_id}: add replica {replica_id}", category="membership")
                        registered_with_gfd = True

                except (socket.timeout, OSError, ConnectionError) as error:
                    log_utils.log(
                        lfd_id,
                        f"[{heartbeat_count}] Heartbeat to {replica_id} FAILED/TIMED OUT ({error}). "
                        f"Replica presumed crashed.",
                        category="failure",
                    )
                    if gfd_link is not None and registered_with_gfd:
                        gfd_link.send_line(protocol.build_member_delete(lfd_id, replica_id))
                        log_utils.log(lfd_id, f"{lfd_id}: delete replica {replica_id}", category="membership")
                        registered_with_gfd = False
                    break

                heartbeat_count += 1
                time.sleep(heartbeat_freq_sec)

        # Connection dropped; pause briefly before attempting to reconnect,
        # in case the replica is restarted later with a new heartbeat_freq.
        time.sleep(heartbeat_freq_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Local Fault Detector that heartbeats a server replica.")
    parser.add_argument("--lfd-id", default="LFD1", help="Unique ID for this LFD (e.g. LFD1).")
    parser.add_argument("--replica-id", default="S1", help="Replica ID this LFD heartbeats (e.g. S1).")
    parser.add_argument("--server-host", default="localhost", help="Host/IP of the server replica.")
    parser.add_argument("--server-port", type=int, default=5000, help="Port of the server replica.")
    parser.add_argument(
        "--heartbeat-freq",
        type=float,
        required=True,
        help="Seconds between heartbeats (to the replica, and to the GFD if configured). "
             "Must be supplied on the command line (no default/hard-coded value).",
    )
    parser.add_argument(
        "--gfd-host",
        default=None,
        help="Host/IP of the GFD (Milestone 2). Omit together with --gfd-port to run in "
             "Milestone-1 mode with no GFD involvement at all.",
    )
    parser.add_argument(
        "--gfd-port",
        type=int,
        default=None,
        help="Port of the GFD (Milestone 2). Omit together with --gfd-host for Milestone-1 mode.",
    )
    parser.add_argument(
        "--replica-public-host",
        default=None,
        help="Milestone 2, required with --gfd-host/--gfd-port: the host/IP that OTHER machines "
             "(clients, on the sacred machine) should use to reach this LFD's replica - i.e. this "
             "machine's own LAN/Tailscale IP, or 'localhost' for a single-laptop demo. Distinct from "
             "--server-host, which is how the LFD itself reaches its co-located replica (normally "
             "'localhost') and is never sent anywhere.",
    )
    args = parser.parse_args()

    gfd_link = None
    if args.gfd_host is not None and args.gfd_port is not None:
        if args.replica_public_host is None:
            raise SystemExit("--replica-public-host is required when --gfd-host/--gfd-port are given, "
                              "so clients on the sacred machine know where to reach this replica.")
        gfd_link = _connect_to_gfd(args.lfd_id, args.gfd_host, args.gfd_port)
        gfd_thread = threading.Thread(
            target=run_gfd_heartbeat_loop,
            args=(args.lfd_id, gfd_link, args.heartbeat_freq),
            daemon=True,
        )
        gfd_thread.start()
    elif args.gfd_host is not None or args.gfd_port is not None:
        raise SystemExit("--gfd-host and --gfd-port must be supplied together (or not at all).")

    run_local_fault_detector(args.lfd_id, args.replica_id, args.server_host, args.server_port, args.heartbeat_freq, gfd_link, args.replica_public_host)


if __name__ == "__main__":
    main()
