# src/net_utils.py
"""
Reusable TCP socket helpers shared by every process.

Why this file exists: M1 only needs one server accepting connections, but M2
triples the number of connections per replica (client links, LFD link, peer
links) and M3 adds a checkpoint channel on top of that. Rather than hand-roll
socket setup/accept-loop code in every process file, we write the two generic
patterns once here: "TCP server that spawns a handler per connection" and
"TCP client that connects and can reconnect." Every later milestone reuses
these unchanged.
"""

import socket
import threading
from typing import Callable

# Without a limit, connecting to a powered-off machine blocks for over a minute.
_CONNECT_TIMEOUT_SEC = 3.0


def start_tcp_server(host: str, port: int, handle_connection: Callable[[socket.socket, tuple], None]) -> socket.socket:
    """Start listening on (host, port) and spawn a thread per accepted connection.

    handle_connection(conn, addr) is called in its own thread for every new
    connection, so the caller just supplies the per-connection logic (e.g.
    "read requests from a client" or "read heartbeats from an LFD") and does
    not need to touch accept-loop or threading code directly.

    Returns the listening socket so the caller can close it on shutdown.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()

    def accept_loop():
        while True:
            try:
                conn, addr = server_socket.accept()
            except OSError:
                # Listening socket was closed (e.g. during shutdown) - stop accepting.
                break
            thread = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
            thread.start()

    accept_thread = threading.Thread(target=accept_loop, daemon=True)
    accept_thread.start()
    return server_socket


def connect_to_server(host: str, port: int) -> socket.socket:
    """Open a single TCP connection to (host, port) and return the socket."""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(_CONNECT_TIMEOUT_SEC)

    try:
        client_socket.connect((host, port))
    except OSError:
        client_socket.close()
        raise

    client_socket.settimeout(None)
    return client_socket


def local_ip_toward(host: str) -> str:
    """Return this machine's IP address on the network that reaches host.

    Connecting a UDP socket only picks a route; it sends no packets.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect((host, 9))
        return probe.getsockname()[0]


def is_loopback(host: str) -> bool:
    """Return whether host names this machine only (localhost, 127.x.x.x)."""
    return socket.gethostbyname(host).startswith("127.")
