"""
S1: the single stateful server replica for Milestone 1.

What this process does: listens for TCP connections from two kinds of peers -
client processes (C1/C2/C3) sending ADD requests, and its Local Fault Detector
(LFD1) sending heartbeats - on the same port. It owns my_state (a shared
counter) and updates it whenever it processes a client request.

Concurrency note: the assignment requires the server to be deterministic (no
threads touching my_state concurrently, no randomness, no timers affecting
state). We accept each connection on its own thread (via net_utils) so slow
clients don't block each other's connections from being accepted, but all
reads/writes of my_state are protected by a single lock so requests are still
applied one at a time, in the order they arrive at the lock - there is no
hidden concurrency in the actual state machine.
"""

import argparse
import threading

import log_utils
import net_utils
import protocol


class ServerReplica:
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.my_state = 0
        # Guards my_state so concurrent client connections still apply
        # requests one at a time, preserving the single-threaded state machine.
        self._state_lock = threading.Lock()

    def run(self, host: str, port: int) -> None:
        net_utils.start_tcp_server(host, port, self._handle_connection)
        log_utils.log(self.replica_id, f"Listening on {host}:{port}", category="lifecycle")

    def _handle_connection(self, conn, addr) -> None:
        """Per-connection loop: keep reading lines until the peer disconnects.

        A single connection is used exclusively by one peer (either one client,
        or the LFD) for its whole lifetime, so we branch on message type as
        each line arrives rather than assuming a fixed peer per connection.

        A connection can die mid-read for reasons that have nothing to do
        with the peer choosing to disconnect - a dropped/reset network path,
        for instance - surfacing as an OSError from receive_line() rather
        than a clean empty-string return. A flaky network can also deliver a
        truncated/garbled line, which protocol.parse_message() always turns
        into a ValueError rather than a raw IndexError (see its docstring).
        Both previously uncaught here, either crashed the connection's
        dedicated thread with a raw traceback on the console; net_utils
        gives every connection its own thread, so it never took down the
        server or any other connection, but it's noisy and unhelpful during
        a demo. Log it cleanly instead, same as every other peer-to-peer
        error path in this codebase already does.
        """
        with conn:
            try:
                while True:
                    line = protocol.receive_line(conn)
                    if not line:
                        break
                    message = protocol.parse_message(line)

                    if message["type"] == protocol.REQUEST:
                        self._handle_request(conn, message)
                    elif message["type"] == protocol.HEARTBEAT:
                        self._handle_heartbeat(conn, message)
            except (OSError, ValueError) as error:
                log_utils.log(self.replica_id, f"Connection from {addr} dropped: {error}", category="failure")

    def _handle_request(self, conn, message: dict) -> None:
        """Apply a client's request to my_state and send back the new value."""
        client_id = message["client_id"]
        request_num = message["request_num"]
        op = message["op"]
        amount = int(message["value"])

        log_utils.log(
            self.replica_id,
            f"Received <{client_id}, {self.replica_id}, {request_num}, request> "
            f"({op} {amount})",
            category="request_reply",
        )

        with self._state_lock:
            log_utils.log(
                self.replica_id,
                f"my_state = {self.my_state} before processing "
                f"<{client_id}, {self.replica_id}, {request_num}, request>",
                category="state",
            )

            if op == "ADD":
                self.my_state += amount
            else:
                raise ValueError(f"Unsupported op: {op!r}")

            log_utils.log(
                self.replica_id,
                f"my_state = {self.my_state} after processing "
                f"<{client_id}, {self.replica_id}, {request_num}, request>",
                category="state",
            )
            new_state = self.my_state

        log_utils.log(
            self.replica_id,
            f"Sending <{client_id}, {self.replica_id}, {request_num}, reply>",
            category="request_reply",
        )
        reply_line = protocol.build_reply(client_id, self.replica_id, request_num, "STATE", new_state)
        protocol.send_line(conn, reply_line)

    def _handle_heartbeat(self, conn, message: dict) -> None:
        """Respond to an LFD heartbeat with an ALIVE message."""
        lfd_id = message["lfd_id"]
        heartbeat_count = message["heartbeat_count"]

        log_utils.log(
            self.replica_id,
            f"Received heartbeat #{heartbeat_count} from {lfd_id}",
            category="heartbeat",
        )

        alive_line = protocol.build_alive(self.replica_id, lfd_id, heartbeat_count)
        protocol.send_line(conn, alive_line)

        log_utils.log(
            self.replica_id,
            f"Sent alive reply #{heartbeat_count} to {lfd_id}",
            category="heartbeat",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a stateful server replica.")
    parser.add_argument("--replica-id", default="S1", help="Unique ID for this replica (e.g. S1).")
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to listen on.")
    parser.add_argument("--port", type=int, default=5000, help="Port to listen on.")
    args = parser.parse_args()

    replica = ServerReplica(args.replica_id)
    replica.run(args.host, args.port)

    # Block the main thread forever; connection-handling happens in the
    # background threads started by net_utils.start_tcp_server. Ctrl-C raises
    # KeyboardInterrupt here, which is exactly the "crash" we want the LFD to
    # detect via a missed heartbeat.
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        log_utils.log(args.replica_id, "Shutting down (Ctrl-C).", category="failure")


if __name__ == "__main__":
    main()
