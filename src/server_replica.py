"""Milestone 1 server replica S1.

S1 is deliberately single-threaded. It handles one short-lived TCP connection at
a time, which keeps the counter update deterministic and is enough for manual
Milestone 1 requests plus LFD heartbeats.
"""

import argparse  # Parses --replica-id, --host, and --port from the terminal.
import socket  # Creates the TCP listener used by S1.
from typing import Any, Dict, Tuple  # Documents received message and address types.

from log_utils import log, request_label  # Reuses consistent project logging helpers.
from protocol import ProtocolError, receive_message, require_fields, send_message


class ServerReplica:
    """A single, stateful server replica for Milestone 1."""

    def __init__(self, replica_id: str, host: str, port: int) -> None:
        # Save the server's unique identity, such as S1.
        self.replica_id = replica_id

        # Save the network interface/address on which S1 will listen.
        self.host = host

        # Save the TCP port on which S1 will listen.
        self.port = port

        # This counter is the required stateful application state.
        self.my_state = 0

    def run(self) -> None:
        """Listen forever and process one incoming message per TCP connection."""

        # Create an IPv4 TCP socket for accepting client and LFD connections.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            # Permit an immediate restart after Ctrl-C during repeated demo runs.
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind S1 to the requested host and port.
            listener.bind((self.host, self.port))

            # Start accepting queued TCP connections; 10 is a small backlog for M1.
            listener.listen(10)

            # Tell the presenter exactly where the server is reachable.
            log(self.replica_id, f"listening on {self.host}:{self.port}; my_state = {self.my_state}")

            try:
                # Keep S1 alive until the user stops it with Ctrl-C.
                while True:
                    # Wait for one client or LFD connection.
                    connection, address = listener.accept()

                    # Always close this short-lived connection after one message/reply.
                    with connection:
                        # Avoid waiting forever if a peer connects but never sends a line.
                        connection.settimeout(5.0)

                        # Handle errors for this connection without stopping S1 itself.
                        self._handle_connection(connection, address)
            except KeyboardInterrupt:
                # Ctrl-C intentionally simulates the S1 crash during the Milestone 1 demo.
                log(self.replica_id, "stopping because Ctrl-C was received")

    def _handle_connection(self, connection: socket.socket, address: Tuple[str, int]) -> None:
        """Read one message and dispatch it to the matching S1 handler."""

        try:
            # Decode the one JSON message sent on this TCP connection.
            message = receive_message(connection)

            # Every project message must state which kind of message it is.
            require_fields(message, "type")

            # Read the message type once so the branching below stays clear.
            message_type = message["type"]

            # Client requests change the stateful application counter.
            if message_type == "REQUEST":
                self._handle_client_request(connection, message)

            # LFD heartbeats do not change my_state; they only test whether S1 is alive.
            elif message_type == "HEARTBEAT":
                self._handle_heartbeat(connection, message)

            # Reject unexpected messages while keeping S1 available for later valid messages.
            else:
                raise ProtocolError(f"unsupported message type: {message_type}")
        except (OSError, ProtocolError, ValueError) as error:
            # Show malformed or disconnected peers in S1's own console for debugging.
            log(self.replica_id, f"connection from {address[0]}:{address[1]} failed: {error}")

    def _handle_client_request(self, connection: socket.socket, message: Dict[str, Any]) -> None:
        """Apply one ADD request, then return the resulting my_state to that client."""

        # Check that this request contains everything S1 needs to identify and apply it.
        require_fields(message, "client_id", "replica_id", "request_num", "operation", "amount")

        # Extract values into named variables for readability in logs and replies.
        client_id = str(message["client_id"])
        destination_replica_id = str(message["replica_id"])
        request_num = int(message["request_num"])
        operation = str(message["operation"])
        amount = int(message["amount"])

        # Reject requests intended for another replica before changing this replica's state.
        if destination_replica_id != self.replica_id:
            raise ProtocolError(
                f"request was intended for {destination_replica_id}, not {self.replica_id}"
            )

        # Milestone 1 intentionally supports only the simple counter increment operation.
        if operation != "ADD":
            raise ProtocolError(f"unsupported client operation: {operation}")

        # Print receipt using the tuple notation required by the project guide.
        label = request_label(client_id, self.replica_id, request_num, "request")
        log(self.replica_id, f"received {label}")

        # Print the pre-update state exactly before processing the incoming request.
        log(self.replica_id, f"my_state_{self.replica_id} = {self.my_state} before processing {label}")

        # Apply the deterministic state transition for the requested counter increment.
        self.my_state += amount

        # Print the updated state before sending the reply back to the client.
        log(self.replica_id, f"my_state_{self.replica_id} = {self.my_state} before returning reply")

        # Build a reply that returns the identifiers needed to match it to the request.
        reply = {
            "type": "REPLY",
            "client_id": client_id,
            "replica_id": self.replica_id,
            "request_num": request_num,
            "state": self.my_state,
        }

        # Send the reply over the same short-lived TCP connection.
        send_message(connection, reply)

        # Log reply transmission after the bytes have been handed to TCP.
        reply_label = request_label(client_id, self.replica_id, request_num, "reply")
        log(self.replica_id, f"sending {reply_label}")

    def _handle_heartbeat(self, connection: socket.socket, message: Dict[str, Any]) -> None:
        """Answer one heartbeat so LFD1 knows S1 is currently reachable."""

        # Heartbeats need the LFD identity, replica identity, and heartbeat sequence number.
        require_fields(message, "lfd_id", "replica_id", "heartbeat_count")

        # Extract fields for validation and human-readable terminal output.
        lfd_id = str(message["lfd_id"])
        destination_replica_id = str(message["replica_id"])
        heartbeat_count = int(message["heartbeat_count"])

        # Make sure an LFD did not accidentally heartbeat a different replica endpoint.
        if destination_replica_id != self.replica_id:
            raise ProtocolError(
                f"heartbeat was intended for {destination_replica_id}, not {self.replica_id}"
            )

        # Make heartbeat receipt visible in the S1 terminal, as required by the rubric.
        log(self.replica_id, f"[{heartbeat_count}] received heartbeat from {lfd_id}")

        # The ALIVE message acknowledges this exact LFD heartbeat count.
        alive_reply = {
            "type": "ALIVE",
            "lfd_id": lfd_id,
            "replica_id": self.replica_id,
            "heartbeat_count": heartbeat_count,
        }

        # Return the alive acknowledgement to LFD1.
        send_message(connection, alive_reply)

        # Make the heartbeat reply visible in the S1 terminal too.
        log(self.replica_id, f"[{heartbeat_count}] sending alive reply to {lfd_id}")


def parse_arguments() -> argparse.Namespace:
    """Define and parse the command-line options for launching S1."""

    # Create the parser that powers the `python3 src/server_replica.py ...` command.
    parser = argparse.ArgumentParser(description="Run one Milestone 1 server replica")

    # Let the same file later run as S1, S2, or S3; M1 uses S1.
    parser.add_argument("--replica-id", default="S1", help="replica identifier, for example S1")

    # 0.0.0.0 accepts connections from the local machine and other LAN machines.
    parser.add_argument("--host", default="0.0.0.0", help="interface on which to listen")

    # Keep the port configurable so the team can avoid conflicts on a shared machine.
    parser.add_argument("--port", type=int, default=5000, help="TCP port on which to listen")

    # Return all parsed settings to main().
    return parser.parse_args()


def main() -> None:
    """Create S1 from command-line settings and start its event loop."""

    # Read the user's launch arguments.
    args = parse_arguments()

    # Construct the one stateful replica process.
    server = ServerReplica(args.replica_id, args.host, args.port)

    # Start accepting client and LFD messages.
    server.run()


# Run main() only when this file is launched directly, not when imported by tests.
if __name__ == "__main__":
    main()
