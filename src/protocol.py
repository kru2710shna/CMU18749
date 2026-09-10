"""
Defines the wire format for every message type exchanged in the system, and the
socket helpers used to send/receive them.

Why this file is centralized and kept generic: this same envelope gets extended
in later milestones (duplicate detection needs request_num tracking, passive
replication needs checkpoint messages, membership changes need GFD/RM fields).
By building requests/replies/heartbeats/alives through one shared set of
functions now, adding a new field or message type later means editing this file
only, instead of hunting through every process file for hand-rolled string
formatting.

Wire format: pipe-delimited text lines, newline-terminated, matching the
suggested convention in the assignment:
    REQUEST|<client_id>|<replica_id>|<request_num>|<op>|<value>
    REPLY|<client_id>|<replica_id>|<request_num>|<op>|<value>
    HEARTBEAT|<lfd_id>|<replica_id>|<heartbeat_count>
    ALIVE|<replica_id>|<lfd_id>|<heartbeat_count>
"""

import socket

# Message type tags used as the first field of every line.
REQUEST = "REQUEST"
REPLY = "REPLY"
HEARTBEAT = "HEARTBEAT"
ALIVE = "ALIVE"

_DELIMITER = "|"
_ENCODING = "utf-8"


def build_request(client_id: str, replica_id: str, request_num: int, op: str, value: str) -> str:
    """Build a REQUEST line, e.g. a client asking a server to ADD 1 to my_state."""
    fields = [REQUEST, client_id, replica_id, str(request_num), op, str(value)]
    return _DELIMITER.join(fields)


def build_reply(client_id: str, replica_id: str, request_num: int, op: str, value: str) -> str:
    """Build a REPLY line, e.g. a server telling a client the new my_state."""
    fields = [REPLY, client_id, replica_id, str(request_num), op, str(value)]
    return _DELIMITER.join(fields)


def build_heartbeat(lfd_id: str, replica_id: str, heartbeat_count: int) -> str:
    """Build a HEARTBEAT line sent from an LFD to the replica it monitors."""
    fields = [HEARTBEAT, lfd_id, replica_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_alive(replica_id: str, lfd_id: str, heartbeat_count: int) -> str:
    """Build an ALIVE line, a replica's response to a heartbeat."""
    fields = [ALIVE, replica_id, lfd_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def parse_message(line: str) -> dict:
    """Parse any of the message types above into a dict keyed by field name.

    Every message type is parsed into a plain dict (rather than distinct classes)
    to keep this generic: new message types (e.g. CHECKPOINT in M3) can be added
    by adding one more branch here without changing the calling code's shape.
    """
    fields = line.strip().split(_DELIMITER)
    message_type = fields[0]

    if message_type in (REQUEST, REPLY):
        return {
            "type": message_type,
            "client_id": fields[1],
            "replica_id": fields[2],
            "request_num": int(fields[3]),
            "op": fields[4],
            "value": fields[5],
        }
    elif message_type == HEARTBEAT:
        return {
            "type": message_type,
            "lfd_id": fields[1],
            "replica_id": fields[2],
            "heartbeat_count": int(fields[3]),
        }
    elif message_type == ALIVE:
        return {
            "type": message_type,
            "replica_id": fields[1],
            "lfd_id": fields[2],
            "heartbeat_count": int(fields[3]),
        }
    else:
        raise ValueError(f"Unknown message type: {message_type!r}")


def send_line(sock: socket.socket, line: str) -> None:
    """Send one newline-terminated message over an already-connected socket."""
    sock.sendall((line + "\n").encode(_ENCODING))


def receive_line(sock: socket.socket, buffer_size: int = 4096) -> str:
    """Block until one newline-terminated message is received.

    Returns an empty string if the peer closed the connection, so callers can
    treat that the same way as a disconnect/failure.
    """
    data = sock.recv(buffer_size)
    if not data:
        return ""
    return data.decode(_ENCODING).strip()
