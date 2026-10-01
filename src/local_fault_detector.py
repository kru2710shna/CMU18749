"""
LFD1: the Local Fault Detector for Milestone 1.

What this process does: opens one persistent TCP connection to its local
server replica (S1) and heartbeats it in a loop, forever, at heartbeat_freq
(a CLI argument, never hard-coded per the spec). It counts heartbeats sent
(heartbeat_count) and detects a crash by timing out while waiting for the
ALIVE reply.

Design note for later milestones: the GFD will heartbeat LFDs, and the RM
will (eventually) sit above the GFD, using this exact same
register-then-heartbeat-in-a-loop pattern. Keeping this loop generic (parameterized
by who it heartbeats and how often) means it can be lifted almost as-is for
LFD -> GFD heartbeating later.
"""

import argparse
import socket
import time

import log_utils
import net_utils
import protocol

# How long to wait for an ALIVE reply before declaring a heartbeat timed out.
# Derived from heartbeat_freq so a slower heartbeat schedule also gets a
# proportionally more patient timeout.
_TIMEOUT_MULTIPLIER = 2.0


def run_local_fault_detector(lfd_id: str, replica_id: str, server_host: str, server_port: int, heartbeat_freq_sec: float) -> None:
    heartbeat_count = 1
    timeout_sec = heartbeat_freq_sec * _TIMEOUT_MULTIPLIER

    while True:
        try:
            sock = net_utils.connect_to_server(server_host, server_port)
            sock.settimeout(timeout_sec)
        except OSError as error:
            log_utils.log(lfd_id, f"Could not connect to {replica_id}: {error}", category="failure")
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

                except (socket.timeout, OSError, ConnectionError) as error:
                    log_utils.log(
                        lfd_id,
                        f"[{heartbeat_count}] Heartbeat to {replica_id} FAILED/TIMED OUT ({error}). "
                        f"Replica presumed crashed.",
                        category="failure",
                    )
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
        help="Seconds between heartbeats. Must be supplied on the command line (no default/hard-coded value).",
    )
    args = parser.parse_args()

    run_local_fault_detector(args.lfd_id, args.replica_id, args.server_host, args.server_port, args.heartbeat_freq)


if __name__ == "__main__":
    main()
