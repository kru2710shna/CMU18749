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

Milestone 2 mode (opt-in via --gfd-host/--gfd-port, or --replicas for a
static/offline alternative): connects to all active replicas at once and,
for every request_num, sends the identical request to every replica
before reading any reply back (per the assignment's notation example),
delivers whichever reply arrives first, and prints "request_num N:
Discarded duplicate reply from Sx" for the rest - this is how the client
tolerates any single replica being down without pausing.

Per the updated Milestone 2 spec, a client should learn which replicas
exist from the GFD rather than being told a static list on the command
line: it registers with the GFD once (CLIENT_HELLO), receives an initial
MEMBERSHIP snapshot, and keeps a background thread listening for further
snapshots so it can open connections to newly-added replicas and drop
connections to removed ones while the main request loop keeps running.
--replicas remains as a static/offline alternative (e.g. for testing
without a GFD running) - it populates the exact same live, lockable
connection registry, just once, instead of continuously from the GFD.
"""

import argparse
import socket
import threading
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
                reply = protocol.parse_message(reply_line)
            except (OSError, ValueError) as error:
                # ValueError covers a truncated/garbled reply (parse_message
                # always raises ValueError for that, never a raw IndexError),
                # on top of the usual connection-error cases.
                log_utils.log(client_id, f"Connection to {replica_id} failed: {error}", category="failure")
                break

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
    list of (replica_id, host, port) tuples."""
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


class ReplicaSet:
    """Thread-safe, live registry of a client's replica connections.

    Two different callers mutate/read this concurrently in GFD mode: the
    main request loop (reads a snapshot each round, marks entries dead on
    error) and the background GFD-listener thread (adds/removes entries
    whenever a fresh MEMBERSHIP broadcast arrives). In static --replicas
    mode there's no listener thread, but it's the same class either way -
    sync_membership() is just called once, up front.
    """

    def __init__(self, client_id: str):
        self.client_id = client_id
        self._lock = threading.Lock()
        self._entries = {}  # replica_id -> [sock, alive]

    def snapshot(self):
        """A point-in-time list of (replica_id, sock, alive) to iterate."""
        with self._lock:
            return [(rid, entry[0], entry[1]) for rid, entry in self._entries.items()]

    def mark_dead(self, replica_id: str) -> None:
        with self._lock:
            if replica_id in self._entries:
                self._entries[replica_id][1] = False

    def any_alive(self) -> bool:
        with self._lock:
            return any(alive for _sock, alive in self._entries.values())

    def sync_membership(self, members) -> None:
        """Reconcile against a fresh (replica_id, host, port) list from the
        GFD (or the static --replicas arg): open connections to replicas we
        don't have a *live* connection for yet (including one we previously
        marked dead - a replica the GFD is currently reporting as a member
        is, by definition, healthy again as far as this client is concerned,
        even if an earlier round's send/recv failed against it), and close
        and drop ones no longer in the list.

        The blocking connect() calls happen *outside* self._lock (only the
        bookkeeping around them is locked), so a slow/unreachable replica
        can't stall the request loop's snapshot()/mark_dead() calls, which
        also need this same lock.
        """
        target_ids = {replica_id for replica_id, _host, _port in members}

        with self._lock:
            current_ids = set(self._entries.keys())
            for replica_id in current_ids - target_ids:
                sock, _alive = self._entries.pop(replica_id)
                try:
                    sock.close()
                except OSError:
                    pass
                log_utils.log(self.client_id, f"Replica {replica_id} left membership; connection closed.", category="membership")

        for replica_id, host, port in members:
            with self._lock:
                existing = self._entries.get(replica_id)
                already_alive = existing is not None and existing[1]
            if already_alive:
                continue

            try:
                sock = net_utils.connect_to_server(host, port)
            except OSError as error:
                log_utils.log(self.client_id, f"Could not connect to {replica_id} at {host}:{port}: {error}", category="failure")
                continue
            log_utils.log(self.client_id, f"Connected to {replica_id} at {host}:{port}", category="lifecycle")

            with self._lock:
                old = self._entries.get(replica_id)
                self._entries[replica_id] = [sock, True]
            if old is not None:
                try:
                    old[0].close()
                except OSError:
                    pass

    def close_all(self) -> None:
        with self._lock:
            for sock, _alive in self._entries.values():
                sock.close()


def _run_multireplica_request_loop(client_id: str, replica_set: ReplicaSet, add_amount: int, request_interval_sec: float, request_count: int, manual_mode: bool, reply_timeout_sec: float) -> None:
    """Milestone 2: fan every request out to every currently-live replica,
    delivering the first reply for each request_num and discarding the
    rest as duplicates - active replication's client-side half.

    Design (chosen deliberately over an "advance on first reply" model):
    each round is self-contained. For request_num N, we (1) send to every
    still-alive replica, back-to-back, before reading anything back, then
    (2) collect whichever replies arrive within reply_timeout_sec per
    replica, deliver the first and discard the rest as duplicates, then
    only then move on to N+1. A replica that times out or errors this
    round is marked dead in the ReplicaSet and skipped in future rounds
    unless a later GFD membership update reintroduces it (GFD mode) -
    Milestone 2 explicitly does not require recovery, so there is no
    reconnect logic of the client's own here (unlike the LFD's replica
    connection, which does reconnect, because the LFD *is* tested on
    recovery scenarios in later milestones).
    """
    request_num = 1
    while request_count <= 0 or request_num <= request_count:
        current_amount = _prompt_for_add_amount(client_id, add_amount) if manual_mode else add_amount
        connections = replica_set.snapshot()

        # Phase 1: send the identical request to every live replica,
        # back-to-back, before reading any reply back.
        for replica_id, sock, alive in connections:
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
                replica_set.mark_dead(replica_id)

        # Phase 2: collect whichever replies show up (bounded wait per
        # replica), deliver the first, discard the rest as duplicates.
        delivered = False
        for replica_id, sock, alive in connections:
            if not alive:
                continue
            try:
                # settimeout() itself can race with the GFD-listener thread
                # closing this exact socket (via sync_membership, if this
                # replica left membership mid-round) - moved inside the try
                # so that race is treated like any other per-replica
                # failure instead of crashing the whole client.
                sock.settimeout(reply_timeout_sec)
                reply_line = protocol.receive_line(sock)
                if not reply_line:
                    raise ConnectionError("Connection closed by replica")
                reply = protocol.parse_message(reply_line)
            except (socket.timeout, OSError, ConnectionError, ValueError) as error:
                # ValueError covers a truncated/garbled reply from this
                # replica (parse_message always raises ValueError for that),
                # treated exactly like any other reason to mark it down.
                log_utils.log(
                    client_id,
                    f"request_num {request_num}: no reply from {replica_id} ({error}); marking it down.",
                    category="failure",
                )
                replica_set.mark_dead(replica_id)
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


def run_client_multireplica(client_id: str, replicas: list, add_amount: int, request_interval_sec: float, request_count: int, manual_mode: bool, reply_timeout_sec: float) -> None:
    """Milestone 2, static mode: the replica list comes once from --replicas
    (no GFD involved) rather than being learned/updated dynamically."""
    replica_set = ReplicaSet(client_id)
    replica_set.sync_membership(replicas)

    if not replica_set.any_alive():
        log_utils.log(client_id, "Could not connect to any replica; exiting.", category="failure")
        return

    try:
        _run_multireplica_request_loop(client_id, replica_set, add_amount, request_interval_sec, request_count, manual_mode, reply_timeout_sec)
    except KeyboardInterrupt:
        raise
    finally:
        replica_set.close_all()


def _connect_to_gfd_for_client(client_id: str, gfd_host: str, gfd_port: int) -> socket.socket:
    """Keep retrying until the GFD connection is up, mirroring the LFD's
    own connect-with-retry loop to the GFD."""
    while True:
        try:
            sock = net_utils.connect_to_server(gfd_host, gfd_port)
            log_utils.log(client_id, f"Connected to GFD at {gfd_host}:{gfd_port}", category="lifecycle")
            return sock
        except OSError as error:
            log_utils.log(client_id, f"Could not connect to GFD: {error}", category="failure")
            time.sleep(1.0)


def _gfd_listener_thread(client_id: str, gfd_sock: socket.socket, replica_set: ReplicaSet) -> None:
    """Runs for the client's whole lifetime: reads every MEMBERSHIP
    broadcast the GFD sends after the initial one, and reconciles the
    live ReplicaSet against it. This is the "GFD-to-client communication
    ... printed at ... the client consoles" the rubric requires.

    Runs for the client's whole lifetime as a daemon thread; when the main
    thread shuts down (--request-count exhausted, or Ctrl-C) it closes
    gfd_sock out from under this thread's blocking receive_line() call,
    which is the normal, expected way this loop ends - not a failure.
    """
    while True:
        try:
            line = protocol.receive_line(gfd_sock)
        except OSError:
            return
        if not line:
            log_utils.log(client_id, "GFD connection closed; no further membership updates will be received.", category="failure")
            return
        try:
            message = protocol.parse_message(line)
        except ValueError as error:
            # A single garbled broadcast shouldn't stop listening for every
            # future one - log it and keep going; the GFD will send another
            # full snapshot on the next membership change regardless.
            log_utils.log(client_id, f"Ignoring malformed membership update from GFD: {error}", category="failure")
            continue
        if message["type"] == protocol.MEMBERSHIP:
            members = message["members"]
            names = ", ".join(rid for rid, _h, _p in members) or "(none)"
            log_utils.log(client_id, f"Received membership update from GFD: {len(members)} member(s): {names}", category="membership")
            replica_set.sync_membership(members)


def _register_with_gfd(client_id: str, gfd_host: str, gfd_port: int):
    """Connect, send CLIENT_HELLO, and wait for the initial MEMBERSHIP
    snapshot - retrying the *whole* handshake (not just the connect) if
    anything about it fails.

    A connection can complete its TCP handshake and then die before a
    single byte of application data crosses it either way - the same
    "connects, then immediately breaks" pattern already seen with replica
    connections on a flaky network. Previously, a failure here (in the
    send or the first receive) was completely unguarded and crashed the
    whole client; now the handshake is retried, matching how every other
    connection-with-retry loop in this codebase already treats the GFD as
    an acceptable, transient single point of failure rather than a fatal
    dependency.

    Returns (gfd_sock, initial_members).
    """
    while True:
        gfd_sock = _connect_to_gfd_for_client(client_id, gfd_host, gfd_port)
        try:
            protocol.send_line(gfd_sock, protocol.build_client_hello(client_id))
            log_utils.log(client_id, "Sent CLIENT_HELLO to GFD; waiting for initial membership.", category="lifecycle")

            first_line = protocol.receive_line(gfd_sock)
            if not first_line:
                raise ConnectionError("GFD closed the connection before sending initial membership")
            first_message = protocol.parse_message(first_line)
            return gfd_sock, first_message["members"]
        except (OSError, ValueError, ConnectionError) as error:
            log_utils.log(client_id, f"Lost connection to GFD during registration ({error}); retrying.", category="failure")
            try:
                gfd_sock.close()
            except OSError:
                pass
            time.sleep(1.0)


def run_client_via_gfd(client_id: str, gfd_host: str, gfd_port: int, add_amount: int, request_interval_sec: float, request_count: int, manual_mode: bool, reply_timeout_sec: float) -> None:
    """Milestone 2, GFD-driven mode (the rubric's required path): register
    with the GFD, bootstrap from its initial membership snapshot, then keep
    a background thread listening for further snapshots for the rest of
    this client's lifetime while the main loop keeps sending requests."""
    gfd_sock, members = _register_with_gfd(client_id, gfd_host, gfd_port)
    names = ", ".join(rid for rid, _h, _p in members) or "(none)"
    log_utils.log(client_id, f"Received initial membership from GFD: {len(members)} member(s): {names}", category="membership")

    replica_set = ReplicaSet(client_id)
    replica_set.sync_membership(members)

    listener = threading.Thread(target=_gfd_listener_thread, args=(client_id, gfd_sock, replica_set), daemon=True)
    listener.start()

    try:
        _run_multireplica_request_loop(client_id, replica_set, add_amount, request_interval_sec, request_count, manual_mode, reply_timeout_sec)
    except KeyboardInterrupt:
        raise
    finally:
        replica_set.close_all()
        gfd_sock.close()


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
        "--gfd-host",
        default=None,
        help="Milestone 2 (required/recommended path): host/IP of the GFD. When given (with "
             "--gfd-port), this client registers with the GFD and learns/updates its replica "
             "connections dynamically from GFD membership broadcasts, instead of a static list.",
    )
    parser.add_argument(
        "--gfd-port",
        type=int,
        default=None,
        help="Milestone 2: port of the GFD. Must be given together with --gfd-host.",
    )
    parser.add_argument(
        "--replicas",
        default=None,
        help="Milestone 2, static/offline alternative to --gfd-host: comma-separated "
             "'replica_id:host:port' triples, e.g. 'S1:host1:5000,S2:host2:5000,S3:host3:5000'. "
             "Ignored if --gfd-host is given. Omit both for Milestone-1 single-replica mode.",
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

    if args.gfd_host is not None or args.gfd_port is not None:
        if args.gfd_host is None or args.gfd_port is None:
            raise SystemExit("--gfd-host and --gfd-port must be supplied together.")

    try:
        if args.gfd_host is not None:
            run_client_via_gfd(
                args.client_id,
                args.gfd_host,
                args.gfd_port,
                args.add_amount,
                args.request_interval,
                args.request_count,
                manual_mode=(args.mode == "manual"),
                reply_timeout_sec=args.reply_timeout_ms / 1000.0,
            )
        elif args.replicas is not None:
            try:
                replicas = _parse_replicas_arg(args.replicas)
            except ValueError as error:
                raise SystemExit(f"Invalid --replicas value: {error}")
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
