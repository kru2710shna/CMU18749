"""
Client process for Milestone 1 (and, additively, Milestone 2) - runs as C1,
C2, or C3 depending on the --client-id argument. All three clients use this
exact same file; only the client_id (and what they choose to send) differs,
and they never talk to each other.

Milestone 1 mode (default, unchanged): connects to a single server replica
and sends ADD requests, printing every request it sends and every reply it
receives. Each request carries a unique, per-client request_num so requests
and replies can always be matched via the tuple <client_id, replica_id,
request_num>.

Milestone 2 mode (opt-in via --replicas): connects to all 3 active replicas
at once and, for every request_num, sends the identical request to every
replica before reading any reply back (per the assignment's notation
example), delivers whichever reply arrives first, and prints
"request_num N: Discarded duplicate reply from Sx" for the rest - this is
how the client tolerates any single replica being down without pausing.
"""

import argparse
import socket
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


def _parse_replicas_arg(replicas_arg: str):
    """Parse '--replicas S1:host1:5000,S2:host2:5001,S3:host3:5002' into a
    list of (replica_id, host, port) tuples, in the order given (which is
    also the order requests are sent and replies are read in every round)."""
    replicas = []
    for entry in replicas_arg.split(","):
        entry = entry.strip()
        if not entry:
            continue
        replica_id, host, port_str = entry.split(":")
        replicas.append((replica_id, host, int(port_str)))
    if not replicas:
        raise ValueError("--replicas was given but contained no entries")
    return replicas


def run_client_multireplica(client_id: str, replicas: list, add_amount: int, request_interval_sec: float, request_count: int, manual_mode: bool, reply_timeout_sec: float) -> None:
    """Milestone 2: fan every request out to all 3 active replicas, delivering
    the first reply for each request_num and discarding the rest as
    duplicates - active replication's client-side half.

    Design (chosen deliberately over an "advance on first reply" model):
    each round is self-contained. For request_num N, we (1) send to every
    still-alive replica, back-to-back, before reading anything back, then
    (2) collect whichever replies arrive within reply_timeout_sec per
    replica, deliver the first and discard the rest as duplicates, then only
    then move on to N+1. A replica that times out or errors this round is
    marked dead and skipped for the rest of this client's lifetime -
    Milestone 2 explicitly does not require recovery, so there is no
    reconnect logic here (unlike the LFD's replica connection, which does
    reconnect, because the LFD *is* tested on recovery scenarios in later
    milestones).
    """
    connections = []  # list of [replica_id, sock, alive] - alive is mutable
    for replica_id, host, port in replicas:
        try:
            sock = net_utils.connect_to_server(host, port)
        except OSError as error:
            log_utils.log(client_id, f"Could not connect to {replica_id} at {host}:{port}: {error}", category="failure")
            continue
        log_utils.log(client_id, f"Connected to {replica_id} at {host}:{port}", category="lifecycle")
        connections.append([replica_id, sock, True])

    if not any(alive for _rid, _sock, alive in connections):
        log_utils.log(client_id, "Could not connect to any replica; exiting.", category="failure")
        return

    request_num = 1
    try:
        while request_count <= 0 or request_num <= request_count:
            current_amount = _prompt_for_add_amount(client_id, add_amount) if manual_mode else add_amount

            # Phase 1: send the identical request to every live replica,
            # back-to-back, before reading any reply back.
            for entry in connections:
                replica_id, sock, alive = entry
                if not alive:
                    continue
                request_line = protocol.build_request(client_id, replica_id, request_num, "ADD", current_amount)
                try:
                    protocol.send_line(sock, request_line)
                    log_utils.log(
                        client_id,
                        f"Sent <{client_id}, {replica_id}, {request_num}, request> (ADD {current_amount})",
                        category="request_reply",
                    )
                except OSError as error:
                    log_utils.log(client_id, f"Connection to {replica_id} failed: {error}", category="failure")
                    entry[2] = False

            # Phase 2: collect whichever replies show up (bounded wait per
            # replica), deliver the first, discard the rest as duplicates.
            delivered = False
            for entry in connections:
                replica_id, sock, alive = entry
                if not alive:
                    continue
                sock.settimeout(reply_timeout_sec)
                try:
                    reply_line = protocol.receive_line(sock)
                    if not reply_line:
                        raise ConnectionError("Connection closed by replica")
                    reply = protocol.parse_message(reply_line)
                except (socket.timeout, OSError, ConnectionError) as error:
                    log_utils.log(
                        client_id,
                        f"request_num {request_num}: no reply from {replica_id} ({error}); marking it down.",
                        category="failure",
                    )
                    entry[2] = False
                    continue

                if not delivered:
                    log_utils.log(
                        client_id,
                        f"Received <{client_id}, {replica_id}, {request_num}, reply> (my_state={reply['value']})",
                        category="request_reply",
                    )
                    delivered = True
                else:
                    log_utils.log(
                        client_id,
                        f"request_num {request_num}: Discarded duplicate reply from {replica_id}",
                        category="duplicate",
                    )

            if not delivered:
                log_utils.log(client_id, f"request_num {request_num}: no replica responded at all.", category="failure")

            request_num += 1
            if not manual_mode and request_interval_sec > 0:
                time.sleep(request_interval_sec)
    except KeyboardInterrupt:
        raise
    finally:
        for _replica_id, sock, _alive in connections:
            sock.close()


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
    parser.add_argument(
        "--replicas",
        default=None,
        help="Milestone 2: comma-separated 'replica_id:host:port' triples, e.g. "
             "'S1:host1:5000,S2:host2:5000,S3:host3:5000'. When given, this client fans every "
             "request out to all listed replicas and discards duplicate replies, ignoring "
             "--replica-id/--server-host/--server-port. Omit for Milestone-1 single-replica mode.",
    )
    parser.add_argument(
        "--reply-timeout-ms",
        type=float,
        default=2000.0,
        help="Milestone 2 only: how long to wait for each replica's reply before treating it as down "
             "for that request (and, from then on, as permanently down - Milestone 2 does not require "
             "recovery).",
    )
    args = parser.parse_args()

    try:
        if args.replicas is not None:
            replicas = _parse_replicas_arg(args.replicas)
            run_client_multireplica(
                args.client_id,
                replicas,
                args.add_amount,
                args.request_interval,
                args.request_count,
                manual_mode=(args.mode == "manual"),
                reply_timeout_sec=args.reply_timeout_ms / 1000.0,
            )
        else:
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
