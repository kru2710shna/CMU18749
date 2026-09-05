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


def run_client(client_id: str, replica_id: str, server_host: str, server_port: int, add_amount: int, request_interval_sec: float, request_count: int) -> None:
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
            request_line = protocol.build_request(client_id, replica_id, request_num, "ADD", add_amount)

            # A dead/killed server can surface as either a clean close (recv
            # returns no data) or an OS-level socket error (e.g. connection
            # reset, broken pipe) depending on timing - both mean the same
            # thing to the client, so both are treated as "server is gone"
            # instead of letting the client crash with a raw traceback.
            try:
                protocol.send_line(sock, request_line)
                log_utils.log(
                    client_id,
                    f"Sent <{client_id}, {replica_id}, {request_num}, request> (ADD {add_amount})",
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
            if request_interval_sec > 0:
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
        )
    except KeyboardInterrupt:
        log_utils.log(args.client_id, "Shutting down (Ctrl-C).", category="lifecycle")


if __name__ == "__main__":
    main()
