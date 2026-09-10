"""
Client process for Milestone 1 - runs as C1, C2, or C3 depending on the
--client-id argument. All three clients use this exact same file; only the
client_id (and what they choose to send) differs, and they never talk to each
other.

What this process does: connects to the server replica S1 and sends ADD
requests, printing every request it sends and every reply it receives. Each
request carries a unique, per-client request_num so requests and replies can
always be matched via the tuple <client_id, replica_id, request_num>.
"""

import argparse
import time

import log_utils
import net_utils
import protocol


def _prompt_for_add_amount(client_id: str, default_amount: int) -> int:
    """Ask the operator for the next ADD amount (manual mode only).

    Blank input reuses default_amount so a manual demo can just hit Enter
    repeatedly; typing 'q' lets the operator stop the client on their own
    terms rather than waiting for Ctrl-C.
    """
    # Some terminals occasionally leak stray escape-sequence bytes into stdin
    # (e.g. a bracketed-paste or cursor-report response) right as the operator
    # is typing. Rather than crash the whole client on one bad keystroke, keep
    # re-prompting until we get 'q' or a valid integer.
    while True:
        raw_input_value = input(f"[{client_id}] Enter ADD amount (default {default_amount}, 'q' to quit): ").strip()
        if raw_input_value.lower() == "q":
            raise KeyboardInterrupt
        if raw_input_value == "":
            return default_amount
        try:
            return int(raw_input_value)
        except ValueError:
            print(f"[{client_id}] Invalid input {raw_input_value!r}, please enter a whole number or 'q'.")


def run_client(client_id: str, replica_id: str, server_host: str, server_port: int, add_amount: int, request_interval_sec: float, request_count: int, manual_mode: bool) -> None:
    try:
        sock = net_utils.connect_to_server(server_host, server_port)
    except OSError as error:
        log_utils.log(client_id, f"Could not connect to {replica_id}: {error}", category="failure")
        return
    log_utils.log(client_id, f"Connected to {replica_id} at {server_host}:{server_port}", category="lifecycle")

    request_num = 1
    with sock:
        # request_count <= 0 means "send forever" (useful for the demo's
        # continuous-loop mode); otherwise stop after request_count requests.
        while request_count <= 0 or request_num <= request_count:
            # Manual mode lets the operator drive each request from the
            # keyboard (per the assignment's "manual client requests are
            # allowed" option), instead of firing automatically on a timer.
            current_amount = _prompt_for_add_amount(client_id, add_amount) if manual_mode else add_amount

            request_line = protocol.build_request(client_id, replica_id, request_num, "ADD", current_amount)

            # A dead/killed server can surface as either a clean close (recv
            # returns no data) or an OS-level socket error (e.g. connection
            # reset, broken pipe) depending on timing - both mean the same
            # thing to the client, so both are treated as "server is gone"
            # instead of letting the client crash with a raw traceback.
            try:
                protocol.send_line(sock, request_line)
                log_utils.log(
                    client_id,
                    f"Sent <{client_id}, {replica_id}, {request_num}, request> (ADD {current_amount})",
                    category="request_reply",
                )

                reply_line = protocol.receive_line(sock)
                if not reply_line:
                    log_utils.log(client_id, f"Connection to {replica_id} closed unexpectedly.", category="failure")
                    break
            except OSError as error:
                log_utils.log(client_id, f"Connection to {replica_id} failed: {error}", category="failure")
                break

            reply = protocol.parse_message(reply_line)
            log_utils.log(
                client_id,
                f"Received <{client_id}, {replica_id}, {request_num}, reply> "
                f"(my_state={reply['value']})",
                category="request_reply",
            )

            request_num += 1
            if not manual_mode and request_interval_sec > 0:
                time.sleep(request_interval_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a client that sends ADD requests to a server replica.")
    parser.add_argument("--client-id", required=True, help="Unique ID for this client (e.g. C1).")
    parser.add_argument("--replica-id", default="S1", help="Replica ID to send requests to (e.g. S1).")
    parser.add_argument("--server-host", default="localhost", help="Host/IP of the server replica.")
    parser.add_argument("--server-port", type=int, default=5000, help="Port of the server replica.")
    parser.add_argument("--add-amount", type=int, default=1, help="Amount to ADD per request.")
    parser.add_argument(
        "--request-interval",
        type=float,
        default=1.0,
        help="Seconds to wait between requests (0 for as fast as possible).",
    )
    parser.add_argument(
        "--request-count",
        type=int,
        default=0,
        help="Number of requests to send. 0 (default) means send forever in a loop.",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "manual"],
        default="auto",
        help="'auto' sends requests on a timer (default); 'manual' prompts for each request from the keyboard.",
    )
    args = parser.parse_args()

    try:
        run_client(
            args.client_id,
            args.replica_id,
            args.server_host,
            args.server_port,
            args.add_amount,
            args.request_interval,
            args.request_count,
            manual_mode=(args.mode == "manual"),
        )
    except KeyboardInterrupt:
        log_utils.log(args.client_id, "Shutting down (Ctrl-C).", category="lifecycle")


if __name__ == "__main__":
    main()
