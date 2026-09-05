"""Milestone 1 Local Fault Detector (LFD1).

LFD1 runs beside S1. It sends a heartbeat at a configurable interval and reports
a failed heartbeat whenever it cannot receive S1's alive reply before timeout.
"""

import argparse  # Parses heartbeat settings from the terminal command.
import socket  # Opens a short TCP heartbeat connection to S1.
import time  # Sleeps between heartbeats at the configured frequency.

from log_utils import log  # Uses the shared timestamped console logger.
from protocol import ProtocolError, receive_message, require_fields, send_message


class LocalFaultDetector:
    """One LFD process that monitors one local server replica."""

    def __init__(
        self,
        lfd_id: str,
        replica_id: str,
        server_host: str,
        server_port: int,
        heartbeat_freq_ms: int,
        timeout_ms: int,
    ) -> None:
        # Store the LFD identity, such as LFD1.
        self.lfd_id = lfd_id

        # Store the replica identity this LFD is responsible for monitoring.
        self.replica_id = replica_id

        # Store the server address; on the same machine this is normally localhost.
        self.server_host = server_host

        # Store the port S1 listens on for heartbeat and client messages.
        self.server_port = server_port

        # Convert the user-supplied frequency to seconds for time.sleep().
        self.heartbeat_freq_seconds = heartbeat_freq_ms / 1000.0

        # Convert the user-supplied timeout to seconds for socket APIs.
        self.timeout_seconds = timeout_ms / 1000.0

        # Start the required heartbeat count at one.
        self.heartbeat_count = 1

    def run(self) -> None:
        """Send heartbeats forever until the user stops LFD1 with Ctrl-C."""

        # Make the chosen heartbeat configuration visible at startup.
        log(
            self.lfd_id,
            (
                f"monitoring {self.replica_id} at {self.server_host}:{self.server_port}; "
                f"heartbeat_freq = {self.heartbeat_freq_seconds:.3f}s"
            ),
        )

        try:
            # Continue monitoring even after individual failed heartbeats.
            while True:
                # Send one heartbeat using the current sequential count.
                self._send_one_heartbeat()

                # Increment after the attempt so every next heartbeat has a new number.
                self.heartbeat_count += 1

                # Wait until the next scheduled heartbeat.
                time.sleep(self.heartbeat_freq_seconds)
        except KeyboardInterrupt:
            # Allow the user to stop the detector cleanly after the demo.
            log(self.lfd_id, "stopping because Ctrl-C was received")

    def _send_one_heartbeat(self) -> None:
        """Send one heartbeat and log either S1's reply or the detected failure."""

        # Build the heartbeat message for the server replica being monitored.
        heartbeat = {
            "type": "HEARTBEAT",
            "lfd_id": self.lfd_id,
            "replica_id": self.replica_id,
            "heartbeat_count": self.heartbeat_count,
        }

        # Show every outgoing heartbeat in the LFD1 terminal.
        log(self.lfd_id, f"[{self.heartbeat_count}] sending heartbeat to {self.replica_id}")

        try:
            # Open a short-lived TCP connection to S1; this call can time out.
            with socket.create_connection(
                (self.server_host, self.server_port), timeout=self.timeout_seconds
            ) as connection:
                # Apply the same timeout while waiting for S1's alive reply.
                connection.settimeout(self.timeout_seconds)

                # Send the heartbeat JSON line.
                send_message(connection, heartbeat)

                # Read S1's one response from this connection.
                alive_reply = receive_message(connection)

            # Ensure the response contains all fields that identify this heartbeat.
            require_fields(alive_reply, "type", "lfd_id", "replica_id", "heartbeat_count")

            # Reject responses that do not acknowledge this exact LFD heartbeat.
            if (
                alive_reply["type"] != "ALIVE"
                or alive_reply["lfd_id"] != self.lfd_id
                or alive_reply["replica_id"] != self.replica_id
                or int(alive_reply["heartbeat_count"]) != self.heartbeat_count
            ):
                raise ProtocolError(f"received a non-matching alive reply: {alive_reply}")

            # Successful receipt means S1 was reachable for this heartbeat attempt.
            log(self.lfd_id, f"[{self.heartbeat_count}] received heartbeat from {self.replica_id}")
        except (OSError, ProtocolError, ValueError) as error:
            # In an asynchronous system this is a timeout-based suspicion, not proof.
            log(
                self.lfd_id,
                (
                    f"[{self.heartbeat_count}] FAILED HEARTBEAT: {self.replica_id} "
                    f"did not reply before timeout ({error})"
                ),
            )


def parse_arguments() -> argparse.Namespace:
    """Define and parse the command-line options for launching LFD1."""

    # Create the command-line parser for the fault detector process.
    parser = argparse.ArgumentParser(description="Run one Milestone 1 Local Fault Detector")

    # Keep the LFD identity configurable for later LFD2 and LFD3 milestones.
    parser.add_argument("--lfd-id", default="LFD1", help="LFD identifier, for example LFD1")

    # Keep the monitored replica configurable even though M1 uses only S1.
    parser.add_argument("--replica-id", default="S1", help="replica identifier to monitor")

    # On the real M1 setup, LFD1 uses localhost because it runs beside S1.
    parser.add_argument("--server-host", default="localhost", help="S1 hostname or IP address")

    # This must match the TCP port used when starting S1.
    parser.add_argument("--server-port", type=int, default=5000, help="S1 TCP port")

    # This is the required configurable heartbeat frequency.
    parser.add_argument(
        "--heartbeat-freq-ms",
        type=int,
        default=1000,
        help="time between heartbeats in milliseconds",
    )

    # The guide requires a timeout failure; making it configurable helps testing.
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=1500,
        help="maximum time to wait for S1's reply in milliseconds",
    )

    # Return parsed values to main().
    return parser.parse_args()


def main() -> None:
    """Validate arguments, create LFD1, and start its heartbeat loop."""

    # Read all values passed in the launch command.
    args = parse_arguments()

    # Reject invalid timing values before starting an infinite loop.
    if args.heartbeat_freq_ms <= 0 or args.timeout_ms <= 0:
        raise SystemExit("--heartbeat-freq-ms and --timeout-ms must both be greater than zero")

    # Create the detector process with its chosen configuration.
    detector = LocalFaultDetector(
        args.lfd_id,
        args.replica_id,
        args.server_host,
        args.server_port,
        args.heartbeat_freq_ms,
        args.timeout_ms,
    )

    # Start the repeating heartbeat loop.
    detector.run()


# Run main() only when this file is launched directly.
if __name__ == "__main__":
    main()
