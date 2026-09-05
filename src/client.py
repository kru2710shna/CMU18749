"""Interactive Milestone 1 client.

Run this same file three times with --client-id C1, C2, and C3. Each process
keeps its own request number and sends manual ADD requests to S1.
"""

import argparse  # Parses client and server settings from the launch command.
import socket  # Opens a TCP connection to S1 for each request.

from log_utils import log, request_label  # Uses shared timestamped project logging.
from protocol import ProtocolError, receive_message, require_fields, send_message


class Client:
    """One independent client process, such as C1."""

    def __init__(
        self,
        client_id: str,
        replica_id: str,
        server_host: str,
        server_port: int,
        timeout_ms: int,
    ) -> None:
        # Store the identity that makes this client distinct from C2 and C3.
        self.client_id = client_id

        # Store the server replica this M1 client should contact.
        self.replica_id = replica_id

        # Store the hostname/IP address of the machine running S1.
        self.server_host = server_host

        # Store the TCP port on which S1 is listening.
        self.server_port = server_port

        # Convert the human-friendly millisecond timeout to seconds for socket APIs.
        self.timeout_seconds = timeout_ms / 1000.0

        # Start request numbering at one, as required for uniquely identifiable requests.
        self.request_num = 1

    def run(self) -> None:
        """Prompt the user for manual ADD requests until the user enters q."""

        # Confirm which logical client is running and where it will send requests.
        log(self.client_id, f"started; sending requests to {self.replica_id} at {self.server_host}:{self.server_port}")

        # Explain the very small user interface used for Milestone 1 manual requests.
        print("Enter an integer to add to S1's counter, or enter q to quit.", flush=True)

        while True:
            try:
                # Read one line from this client's own terminal.
                raw_input = input(f"{self.client_id} amount> ").strip()
            except EOFError:
                # End cleanly if the terminal input stream is closed.
                log(self.client_id, "input closed; stopping client")
                return

            # Let the user exit this client process without affecting C2/C3/S1/LFD1.
            if raw_input.lower() in {"q", "quit", "exit"}:
                log(self.client_id, "stopping client")
                return

            try:
                # Convert the user's text into the integer increment S1 should apply.
                amount = int(raw_input)
            except ValueError:
                # Keep this client alive if the user types a non-integer value.
                print("Please enter an integer, q, quit, or exit.", flush=True)
                continue

            # Send exactly one numbered request and display S1's response.
            self._send_add_request(amount)

    def _send_add_request(self, amount: int) -> None:
        """Send one ADD request to S1 and validate the matching reply."""

        # Keep the current number so logs and reply validation refer to the same request.
        current_request_num = self.request_num

        # Build the JSON request that S1 understands.
        request = {
            "type": "REQUEST",
            "client_id": self.client_id,
            "replica_id": self.replica_id,
            "request_num": current_request_num,
            "operation": "ADD",
            "amount": amount,
        }

        # Prepare the rubric's tuple notation for this outgoing request.
        sent_label = request_label(self.client_id, self.replica_id, current_request_num, "request")

        try:
            # Open a TCP connection to S1 with a bounded connection timeout.
            with socket.create_connection(
                (self.server_host, self.server_port), timeout=self.timeout_seconds
            ) as connection:
                # Apply the same timeout to the later reply read.
                connection.settimeout(self.timeout_seconds)

                # Send the serialized request to S1.
                send_message(connection, request)

                # Show that this client sent its request, as required by the rubric.
                log(self.client_id, f"sent {sent_label}")

                # Wait for the one reply S1 sends on this connection.
                reply = receive_message(connection)

            # Verify that the server reply includes the required matching identifiers.
            require_fields(reply, "type", "client_id", "replica_id", "request_num", "state")

            # Reject a reply that cannot be matched safely to this request.
            if (
                reply["type"] != "REPLY"
                or reply["client_id"] != self.client_id
                or reply["replica_id"] != self.replica_id
                or int(reply["request_num"]) != current_request_num
            ):
                raise ProtocolError(f"received a non-matching reply: {reply}")

            # Print the matching reply tuple and the new state returned by S1.
            reply_label = request_label(self.client_id, self.replica_id, current_request_num, "reply")
            log(self.client_id, f"received {reply_label}; S1 reported my_state = {reply['state']}")

            # Advance only after a successful request/reply exchange.
            self.request_num += 1
        except (OSError, ProtocolError, ValueError) as error:
            # Keep the client process alive if S1 is unavailable or a message is malformed.
            log(self.client_id, f"request {current_request_num} failed: {error}")


def parse_arguments() -> argparse.Namespace:
    """Define and parse the command-line options for C1/C2/C3."""

    # Build the command-line parser for this client process.
    parser = argparse.ArgumentParser(description="Run one Milestone 1 client")

    # This is the only argument that changes among C1, C2, and C3 launches.
    parser.add_argument("--client-id", required=True, help="client identifier: C1, C2, or C3")

    # M1 contacts only S1, but keeping this configurable prepares the later milestones.
    parser.add_argument("--replica-id", default="S1", help="destination replica identifier")

    # Use localhost on one laptop, or the server machine's LAN IP on two machines.
    parser.add_argument("--server-host", default="localhost", help="S1 hostname or IP address")

    # Use the same TCP port chosen when starting S1.
    parser.add_argument("--server-port", type=int, default=5000, help="S1 TCP port")

    # Bound each connect/read operation so a dead S1 cannot freeze this client forever.
    parser.add_argument("--timeout-ms", type=int, default=2000, help="socket timeout in milliseconds")

    # Return parsed values to main().
    return parser.parse_args()


def main() -> None:
    """Create the selected client process and start its manual request loop."""

    # Read the launch settings from the terminal command.
    args = parse_arguments()

    # Create one independent logical client.
    client = Client(
        args.client_id,
        args.replica_id,
        args.server_host,
        args.server_port,
        args.timeout_ms,
    )

    # Start the interactive request loop.
    client.run()


# Run main() only when this script is launched directly.
if __name__ == "__main__":
    main()
