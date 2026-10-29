"""
Client process for Milestones 1, 2, and 3.

M1:
    Connect to one replica and send ADD requests.

M2 active replication:
    Connect to every healthy replica, send each request to all of them,
    deliver the first reply, and discard duplicate replies.

M3 passive replication:
    Learn all replicas from the GFD and open connections to them, but send
    requests only to the primary replica (S1 by default). Only S1 replies.
"""

import argparse
import socket
import threading
import time

import log_utils
import net_utils
import protocol


def _prompt_for_add_amount(client_id: str, default_amount: int) -> int:
    """Read one manual ADD amount, or use Enter for the default."""
    while True:
        raw_input_value = input(
            f"[{client_id}] Enter ADD amount "
            f"(default {default_amount}, 'q' to quit): "
        ).strip()

        if raw_input_value.lower() == "q":
            raise KeyboardInterrupt

        if raw_input_value == "":
            return default_amount

        try:
            return int(raw_input_value)
        except ValueError:
            print(
                f"[{client_id}] Invalid input {raw_input_value!r}; "
                "enter an integer or 'q'."
            )


def run_client(
    client_id: str,
    replica_id: str,
    server_host: str,
    server_port: int,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
) -> None:
    """Milestone 1 single-replica client path."""
    try:
        sock = net_utils.connect_to_server(server_host, server_port)
    except OSError as error:
        log_utils.log(
            client_id,
            f"Could not connect to {replica_id}: {error}",
            category="failure",
        )
        return

    log_utils.log(
        client_id,
        f"Connected to {replica_id} at {server_host}:{server_port}",
        category="lifecycle",
    )

    request_num = 1

    with sock:
        while request_count <= 0 or request_num <= request_count:
            current_amount = (
                _prompt_for_add_amount(client_id, add_amount)
                if manual_mode
                else add_amount
            )

            request_line = protocol.build_request(
                client_id,
                replica_id,
                request_num,
                "ADD",
                current_amount,
            )

            try:
                protocol.send_line(sock, request_line)

                log_utils.log(
                    client_id,
                    f"Sent <{client_id}, {replica_id}, "
                    f"{request_num}, request> (ADD {current_amount})",
                    category="request_reply",
                )

                reply_line = protocol.receive_line(sock)

                if not reply_line:
                    raise ConnectionError(
                        f"Connection to {replica_id} closed unexpectedly."
                    )

                reply = protocol.parse_message(reply_line)

            except (OSError, ConnectionError, ValueError) as error:
                log_utils.log(
                    client_id,
                    f"Connection to {replica_id} failed: {error}",
                    category="failure",
                )
                break

            log_utils.log(
                client_id,
                f"Received <{client_id}, {replica_id}, "
                f"{request_num}, reply> "
                f"(my_state={reply['value']})",
                category="request_reply",
            )

            request_num += 1

            if not manual_mode and request_interval_sec > 0:
                time.sleep(request_interval_sec)


def _parse_replicas_arg(replicas_arg: str):
    """
    Parse:

        S1:host1:5000,S2:host2:5000,S3:host3:5000
    """
    replicas = []

    for entry in replicas_arg.split(","):
        entry = entry.strip()

        if not entry:
            continue

        try:
            replica_id, host, port_str = entry.split(":")
            replicas.append((replica_id, host, int(port_str)))
        except ValueError as error:
            raise ValueError(
                f"Invalid replica entry {entry!r}. "
                "Expected S1:host:port."
            ) from error

    if not replicas:
        raise ValueError("--replicas was given but contained no entries.")

    return replicas


class ReplicaSet:
    """
    Thread-safe registry of sockets to replicas.

    In M2/M3 GFD mode, the background GFD listener updates this registry
    whenever membership changes. In static mode, it is populated once from
    --replicas.
    """

    def __init__(self, client_id: str):
        self.client_id = client_id
        self._lock = threading.Lock()

        # replica_id -> [socket, alive]
        self._entries = {}

    def snapshot(self):
        """Return a point-in-time list of (replica_id, socket, alive)."""
        with self._lock:
            return [
                (replica_id, entry[0], entry[1])
                for replica_id, entry in self._entries.items()
            ]

    def mark_dead(self, replica_id: str) -> None:
        with self._lock:
            if replica_id in self._entries:
                self._entries[replica_id][1] = False

    def any_alive(self) -> bool:
        with self._lock:
            return any(
                alive
                for _sock, alive in self._entries.values()
            )

    def sync_membership(self, members) -> None:
        """
        Reconcile client sockets with a full membership snapshot from GFD.

        In M3, this means clients initially open connections to S1/S2/S3.
        The M3 request loop itself sends traffic only to primary S1.
        """
        target_ids = {
            replica_id
            for replica_id, _host, _port in members
        }

        with self._lock:
            current_ids = set(self._entries.keys())

            for replica_id in current_ids - target_ids:
                sock, _alive = self._entries.pop(replica_id)

                try:
                    sock.close()
                except OSError:
                    pass

                log_utils.log(
                    self.client_id,
                    f"Replica {replica_id} left membership; "
                    "connection closed.",
                    category="membership",
                )

        for replica_id, host, port in members:
            with self._lock:
                existing = self._entries.get(replica_id)
                already_alive = (
                    existing is not None
                    and existing[1]
                )

            if already_alive:
                continue

            try:
                sock = net_utils.connect_to_server(host, port)

            except OSError as error:
                log_utils.log(
                    self.client_id,
                    f"Could not connect to {replica_id} "
                    f"at {host}:{port}: {error}",
                    category="failure",
                )
                continue

            log_utils.log(
                self.client_id,
                f"Connected to {replica_id} at {host}:{port}",
                category="lifecycle",
            )

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
            sockets = [
                sock
                for sock, _alive in self._entries.values()
            ]
            self._entries.clear()

        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


def _run_multireplica_request_loop(
    client_id: str,
    replica_set: ReplicaSet,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
) -> None:
    """
    M2 active replication:
    send identical requests to every healthy replica, deliver the first
    reply, and visibly discard duplicate replies.
    """
    request_num = 1

    while request_count <= 0 or request_num <= request_count:
        current_amount = (
            _prompt_for_add_amount(client_id, add_amount)
            if manual_mode
            else add_amount
        )

        connections = replica_set.snapshot()

        # Send request_num N to every healthy active replica first.
        for replica_id, sock, alive in connections:
            if not alive:
                continue

            request_line = protocol.build_request(
                client_id,
                replica_id,
                request_num,
                "ADD",
                current_amount,
            )

            try:
                protocol.send_line(sock, request_line)

                log_utils.log(
                    client_id,
                    f"Sent <{client_id}, {replica_id}, "
                    f"{request_num}, request> "
                    f"(ADD {current_amount})",
                    category="request_reply",
                )

            except OSError as error:
                replica_set.mark_dead(replica_id)

                log_utils.log(
                    client_id,
                    f"Connection to {replica_id} failed: {error}",
                    category="failure",
                )

        # Deliver first reply; visibly discard duplicates.
        delivered = False

        for replica_id, sock, alive in connections:
            if not alive:
                continue

            try:
                sock.settimeout(reply_timeout_sec)

                reply_line = protocol.receive_line(sock)

                if not reply_line:
                    raise ConnectionError(
                        "Connection closed by replica."
                    )

                reply = protocol.parse_message(reply_line)

                if reply["type"] != protocol.REPLY:
                    raise ValueError(
                        f"Expected REPLY, received {reply['type']!r}"
                    )

            except (
                socket.timeout,
                OSError,
                ConnectionError,
                ValueError,
            ) as error:
                replica_set.mark_dead(replica_id)

                log_utils.log(
                    client_id,
                    f"request_num {request_num}: "
                    f"no reply from {replica_id} ({error}); "
                    "marking it down.",
                    category="failure",
                )
                continue

            if not delivered:
                delivered = True

                log_utils.log(
                    client_id,
                    f"Received <{client_id}, {replica_id}, "
                    f"{request_num}, reply> "
                    f"(my_state={reply['value']})",
                    category="request_reply",
                )

            else:
                log_utils.log(
                    client_id,
                    f"request_num {request_num}: "
                    f"Discarded duplicate reply from {replica_id}",
                    category="duplicate",
                )

        if not delivered:
            log_utils.log(
                client_id,
                f"request_num {request_num}: "
                "no replica responded at all.",
                category="failure",
            )

        request_num += 1

        if not manual_mode and request_interval_sec > 0:
            time.sleep(request_interval_sec)


def _run_passive_request_loop(
    client_id: str,
    replica_set: ReplicaSet,
    primary_id: str,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
) -> None:
    """
    M3 passive replication.

    The client may have sockets to S1/S2/S3, but sends requests only to
    primary_id. Therefore only the primary can produce a reply.
    """
    request_num = 1

    while request_count <= 0 or request_num <= request_count:
        primary_connection = None

        for replica_id, sock, alive in replica_set.snapshot():
            if replica_id == primary_id and alive:
                primary_connection = sock
                break

        if primary_connection is None:
            log_utils.log(
                client_id,
                f"Primary {primary_id} is not reachable; waiting.",
                category="failure",
            )

            time.sleep(
                min(max(request_interval_sec, 0.1), 1.0)
            )
            continue

        current_amount = (
            _prompt_for_add_amount(client_id, add_amount)
            if manual_mode
            else add_amount
        )

        request_line = protocol.build_request(
            client_id,
            primary_id,
            request_num,
            "ADD",
            current_amount,
        )

        try:
            protocol.send_line(primary_connection, request_line)

            log_utils.log(
                client_id,
                f"Sent <{client_id}, {primary_id}, "
                f"{request_num}, request> "
                f"(ADD {current_amount})",
                category="request_reply",
            )

            primary_connection.settimeout(reply_timeout_sec)

            reply_line = protocol.receive_line(primary_connection)

            if not reply_line:
                raise ConnectionError(
                    "Primary closed the connection."
                )

            reply = protocol.parse_message(reply_line)

            if reply["type"] != protocol.REPLY:
                raise ValueError(
                    f"Expected REPLY, received {reply['type']!r}"
                )

            if reply["replica_id"] != primary_id:
                raise ValueError(
                    f"Expected reply from primary {primary_id}, "
                    f"received reply from {reply['replica_id']}."
                )

        except (
            socket.timeout,
            OSError,
            ConnectionError,
            ValueError,
        ) as error:
            replica_set.mark_dead(primary_id)

            log_utils.log(
                client_id,
                f"request_num {request_num}: "
                f"request to primary {primary_id} failed ({error}).",
                category="failure",
            )
            continue

        log_utils.log(
            client_id,
            f"Received <{client_id}, {primary_id}, "
            f"{request_num}, reply> "
            f"(my_state={reply['value']})",
            category="request_reply",
        )

        request_num += 1

        if not manual_mode and request_interval_sec > 0:
            time.sleep(request_interval_sec)


def run_client_multireplica(
    client_id: str,
    replicas: list,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
    replication_mode: str = "active",
    primary_id: str = "S1",
) -> None:
    """Run M2 or M3 using a static --replicas list."""
    replica_set = ReplicaSet(client_id)
    replica_set.sync_membership(replicas)

    if not replica_set.any_alive():
        log_utils.log(
            client_id,
            "Could not connect to any replica; exiting.",
            category="failure",
        )
        return

    try:
        if replication_mode == "active":
            _run_multireplica_request_loop(
                client_id,
                replica_set,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

        else:
            _run_passive_request_loop(
                client_id,
                replica_set,
                primary_id,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

    finally:
        replica_set.close_all()


def _connect_to_gfd_for_client(
    client_id: str,
    gfd_host: str,
    gfd_port: int,
) -> socket.socket:
    """Retry until the client successfully connects to the GFD."""
    while True:
        try:
            sock = net_utils.connect_to_server(
                gfd_host,
                gfd_port,
            )

            log_utils.log(
                client_id,
                f"Connected to GFD at {gfd_host}:{gfd_port}",
                category="lifecycle",
            )

            return sock

        except OSError as error:
            log_utils.log(
                client_id,
                f"Could not connect to GFD: {error}",
                category="failure",
            )
            time.sleep(1.0)


def _register_with_gfd(
    client_id: str,
    gfd_host: str,
    gfd_port: int,
):
    """Receive the initial full GFD membership snapshot."""
    while True:
        gfd_sock = _connect_to_gfd_for_client(
            client_id,
            gfd_host,
            gfd_port,
        )

        try:
            protocol.send_line(
                gfd_sock,
                protocol.build_client_hello(client_id),
            )

            log_utils.log(
                client_id,
                "Sent CLIENT_HELLO to GFD; "
                "waiting for initial membership.",
                category="lifecycle",
            )

            first_line = protocol.receive_line(gfd_sock)

            if not first_line:
                raise ConnectionError(
                    "GFD closed before initial membership."
                )

            first_message = protocol.parse_message(first_line)

            if first_message["type"] != protocol.MEMBERSHIP:
                raise ValueError(
                    "Expected initial MEMBERSHIP message "
                    f"but received {first_message['type']!r}."
                )

            return gfd_sock, first_message["members"]

        except (OSError, ConnectionError, ValueError) as error:
            log_utils.log(
                client_id,
                f"GFD registration failed ({error}); retrying.",
                category="failure",
            )

            try:
                gfd_sock.close()
            except OSError:
                pass

            time.sleep(1.0)


def _gfd_listener_thread(
    client_id: str,
    gfd_sock: socket.socket,
    replica_set: ReplicaSet,
) -> None:
    """
    Receive future GFD membership snapshots.

    In M3, when S2 fails, this removes only S2's idle client connection;
    C1/C2/C3 continue using their S1 connection without pausing.
    """
    while True:
        try:
            line = protocol.receive_line(gfd_sock)

            if not line:
                log_utils.log(
                    client_id,
                    "GFD connection closed; no more membership updates.",
                    category="failure",
                )
                return

            message = protocol.parse_message(line)

        except (OSError, ValueError) as error:
            log_utils.log(
                client_id,
                f"GFD listener stopped: {error}",
                category="failure",
            )
            return

        if message["type"] == protocol.MEMBERSHIP:
            members = message["members"]

            names = ", ".join(
                replica_id
                for replica_id, _host, _port in members
            ) or "(none)"

            log_utils.log(
                client_id,
                f"Received membership update from GFD: "
                f"{len(members)} member(s): {names}",
                category="membership",
            )

            replica_set.sync_membership(members)


def run_client_via_gfd(
    client_id: str,
    gfd_host: str,
    gfd_port: int,
    add_amount: int,
    request_interval_sec: float,
    request_count: int,
    manual_mode: bool,
    reply_timeout_sec: float,
    replication_mode: str = "active",
    primary_id: str = "S1",
) -> None:
    """Run M2 active or M3 passive mode using dynamic GFD membership."""
    gfd_sock, members = _register_with_gfd(
        client_id,
        gfd_host,
        gfd_port,
    )

    replica_set = ReplicaSet(client_id)
    replica_set.sync_membership(members)

    names = ", ".join(
        replica_id
        for replica_id, _host, _port in members
    ) or "(none)"

    log_utils.log(
        client_id,
        f"Received initial membership from GFD: "
        f"{len(members)} member(s): {names}",
        category="membership",
    )

    listener = threading.Thread(
        target=_gfd_listener_thread,
        args=(client_id, gfd_sock, replica_set),
        daemon=True,
    )
    listener.start()

    try:
        if replication_mode == "active":
            _run_multireplica_request_loop(
                client_id,
                replica_set,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

        else:
            _run_passive_request_loop(
                client_id,
                replica_set,
                primary_id,
                add_amount,
                request_interval_sec,
                request_count,
                manual_mode,
                reply_timeout_sec,
            )

    finally:
        replica_set.close_all()

        try:
            gfd_sock.close()
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an M1/M2/M3 client."
    )

    parser.add_argument(
        "--client-id",
        required=True,
        help="Unique client ID, for example C1.",
    )

    parser.add_argument(
        "--replica-id",
        default="S1",
        help="M1 target replica ID.",
    )

    parser.add_argument(
        "--server-host",
        default="localhost",
        help="M1 target server host/IP.",
    )

    parser.add_argument(
        "--server-port",
        type=int,
        default=5000,
        help="M1 target server port.",
    )

    parser.add_argument(
        "--add-amount",
        type=int,
        default=1,
        help="Amount added by every request.",
    )

    parser.add_argument(
        "--request-interval",
        type=float,
        default=1.0,
        help="Seconds between automatic requests.",
    )

    parser.add_argument(
        "--request-count",
        type=int,
        default=0,
        help="0 means send forever.",
    )

    parser.add_argument(
        "--mode",
        choices=["auto", "manual"],
        default="auto",
    )

    parser.add_argument(
        "--gfd-host",
        default=None,
        help="M2/M3 GFD host/IP.",
    )

    parser.add_argument(
        "--gfd-port",
        type=int,
        default=None,
        help="M2/M3 GFD port.",
    )

    parser.add_argument(
        "--replicas",
        default=None,
        help=(
            "Static M2/M3 alternative: "
            "S1:host:port,S2:host:port,S3:host:port"
        ),
    )

    parser.add_argument(
        "--reply-timeout-ms",
        type=float,
        default=2000.0,
    )

    parser.add_argument(
        "--replication-mode",
        choices=["active", "passive"],
        default="active",
        help="active=M2; passive=M3.",
    )

    parser.add_argument(
        "--primary-id",
        default="S1",
        help="M3 passive primary replica ID.",
    )

    args = parser.parse_args()

    if (
        args.gfd_host is not None
        and args.gfd_port is None
    ) or (
        args.gfd_host is None
        and args.gfd_port is not None
    ):
        raise SystemExit(
            "--gfd-host and --gfd-port must be supplied together."
        )

    if args.reply_timeout_ms <= 0:
        raise SystemExit(
            "--reply-timeout-ms must be positive."
        )

    try:
        # M2/M3 with dynamic GFD membership.
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
                replication_mode=args.replication_mode,
                primary_id=args.primary_id,
            )

        # M2/M3 static membership alternative.
        elif args.replicas is not None:
            replicas = _parse_replicas_arg(args.replicas)

            run_client_multireplica(
                args.client_id,
                replicas,
                args.add_amount,
                args.request_interval,
                args.request_count,
                manual_mode=(args.mode == "manual"),
                reply_timeout_sec=args.reply_timeout_ms / 1000.0,
                replication_mode=args.replication_mode,
                primary_id=args.primary_id,
            )

        # Existing M1 single-server mode.
        elif args.replication_mode == "active":
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

        else:
            raise SystemExit(
                "M3 passive mode requires --gfd-host/--gfd-port "
                "or --replicas."
            )

    except KeyboardInterrupt:
        log_utils.log(
            args.client_id,
            "Shutting down (Ctrl-C).",
            category="lifecycle",
        )


if __name__ == "__main__":
    main()