"""
Shared wire protocol for Milestones 1 through 4.

M1:
    REQUEST|<client_id>|<replica_id>|<request_num>|<op>|<value>
    REPLY|<client_id>|<replica_id>|<request_num>|<op>|<value>|<stable_request_num>
    HEARTBEAT|<lfd_id>|<replica_id>|<heartbeat_count>
    ALIVE|<replica_id>|<lfd_id>|<heartbeat_count>

M2:
    GFD_HELLO|<lfd_id>|<heartbeat_count>
    GFD_ACK|<lfd_id>|<heartbeat_count>
    MEMBER_ADD|<lfd_id>|<replica_id>|<public_host>|<public_port>
    MEMBER_DELETE|<lfd_id>|<replica_id>
    CLIENT_HELLO|<client_id>
    MEMBERSHIP|<replica_id>:<host>:<port>,...|<primary_id>

M3:
    CHECKPOINT|<sender_id>|<checkpoint_num>|<my_state>|<client_id>:<high_watermark>,...

M4:
    VIEW|<view_num>|<primary_id>|<ready_id>,...|<replica_id>:<host>:<port>,...
    REPLICA_HELLO|<replica_id>
    REPLICA_STATUS|<replica_id>|<i_am_ready>

<stable_request_num> tells a passive client which of its requests are covered
by a checkpoint the backups hold. <primary_id> is empty in active replication.
"""

import socket
import threading
import weakref


REQUEST = "REQUEST"
REPLY = "REPLY"
HEARTBEAT = "HEARTBEAT"
ALIVE = "ALIVE"

GFD_HELLO = "GFD_HELLO"
GFD_ACK = "GFD_ACK"
MEMBER_ADD = "MEMBER_ADD"
MEMBER_DELETE = "MEMBER_DELETE"
CLIENT_HELLO = "CLIENT_HELLO"
MEMBERSHIP = "MEMBERSHIP"

CHECKPOINT = "CHECKPOINT"

VIEW = "VIEW"
REPLICA_HELLO = "REPLICA_HELLO"
REPLICA_STATUS = "REPLICA_STATUS"

_DELIMITER = "|"
_ENCODING = "utf-8"

# TCP is a byte stream: one recv() can return several messages or part of one.
# Bytes received past the end of the current line wait here for the next call.
_receive_buffers = weakref.WeakKeyDictionary()
_receive_buffers_lock = threading.Lock()


def build_request(
    client_id: str,
    replica_id: str,
    request_num: int,
    op: str,
    value: str,
) -> str:
    fields = [REQUEST, client_id, replica_id, str(request_num), op, str(value)]
    return _DELIMITER.join(fields)


def build_reply(
    client_id: str,
    replica_id: str,
    request_num: int,
    op: str,
    value: str,
    stable_request_num: int = 0,
) -> str:
    fields = [
        REPLY,
        client_id,
        replica_id,
        str(request_num),
        op,
        str(value),
        str(stable_request_num),
    ]
    return _DELIMITER.join(fields)


def build_heartbeat(
    lfd_id: str,
    replica_id: str,
    heartbeat_count: int,
) -> str:
    fields = [HEARTBEAT, lfd_id, replica_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_alive(
    replica_id: str,
    lfd_id: str,
    heartbeat_count: int,
) -> str:
    fields = [ALIVE, replica_id, lfd_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_gfd_hello(lfd_id: str, heartbeat_count: int) -> str:
    fields = [GFD_HELLO, lfd_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_gfd_ack(lfd_id: str, heartbeat_count: int) -> str:
    fields = [GFD_ACK, lfd_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_member_add(
    lfd_id: str,
    replica_id: str,
    public_host: str,
    public_port: int,
) -> str:
    fields = [MEMBER_ADD, lfd_id, replica_id, public_host, str(public_port)]
    return _DELIMITER.join(fields)


def build_member_delete(lfd_id: str, replica_id: str) -> str:
    fields = [MEMBER_DELETE, lfd_id, replica_id]
    return _DELIMITER.join(fields)


def build_client_hello(client_id: str) -> str:
    return _DELIMITER.join([CLIENT_HELLO, client_id])


def _format_members(members) -> str:
    return ",".join(
        f"{replica_id}:{host}:{port}"
        for replica_id, host, port in members
    )


def _parse_members(field: str) -> list:
    members = []

    if field:
        for entry in field.split(","):
            replica_id, host, port_str = entry.split(":")
            members.append((replica_id, host, int(port_str)))

    return members


def build_membership(members, primary_id: str = None) -> str:
    return _DELIMITER.join(
        [MEMBERSHIP, _format_members(members), primary_id or ""]
    )


def build_checkpoint(
    sender_id: str,
    checkpoint_num: int,
    my_state: int,
    high_watermarks: dict = None,
) -> str:
    """Build a state checkpoint: my_state plus each client's applied request."""
    watermarks = ",".join(
        f"{client_id}:{request_num}"
        for client_id, request_num in (high_watermarks or {}).items()
    )
    fields = [
        CHECKPOINT,
        sender_id,
        str(checkpoint_num),
        str(my_state),
        watermarks,
    ]
    return _DELIMITER.join(fields)


def build_view(view_num: int, primary_id: str, ready, members) -> str:
    """Build the RM's decision about who is primary and who is ready."""
    fields = [
        VIEW,
        str(view_num),
        primary_id or "",
        ",".join(ready),
        _format_members(members),
    ]
    return _DELIMITER.join(fields)


def build_replica_hello(replica_id: str) -> str:
    return _DELIMITER.join([REPLICA_HELLO, replica_id])


def build_replica_status(replica_id: str, i_am_ready: int) -> str:
    return _DELIMITER.join([REPLICA_STATUS, replica_id, str(i_am_ready)])


def parse_message(line: str) -> dict:
    """Parse one complete protocol message into a dictionary."""
    try:
        return _parse_message_fields(line)
    except (IndexError, ValueError) as error:
        raise ValueError(f"Malformed message: {line!r}") from error


def _parse_message_fields(line: str) -> dict:
    fields = line.strip().split(_DELIMITER)
    message_type = fields[0]

    if message_type in (REQUEST, REPLY):
        message = {
            "type": message_type,
            "client_id": fields[1],
            "replica_id": fields[2],
            "request_num": int(fields[3]),
            "op": fields[4],
            "value": fields[5],
        }

        if message_type == REPLY:
            message["stable_request_num"] = int(fields[6])

        return message

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

    elif message_type in (GFD_HELLO, GFD_ACK):
        return {
            "type": message_type,
            "lfd_id": fields[1],
            "heartbeat_count": int(fields[2]),
        }

    elif message_type == MEMBER_ADD:
        return {
            "type": message_type,
            "lfd_id": fields[1],
            "replica_id": fields[2],
            "public_host": fields[3],
            "public_port": int(fields[4]),
        }

    elif message_type == MEMBER_DELETE:
        return {
            "type": message_type,
            "lfd_id": fields[1],
            "replica_id": fields[2],
        }

    elif message_type == CLIENT_HELLO:
        return {
            "type": message_type,
            "client_id": fields[1],
        }

    elif message_type == MEMBERSHIP:
        return {
            "type": message_type,
            "members": _parse_members(fields[1]),
            "primary_id": fields[2] or None,
        }

    elif message_type == CHECKPOINT:
        high_watermarks = {}

        if fields[4]:
            for entry in fields[4].split(","):
                client_id, request_num = entry.split(":")
                high_watermarks[client_id] = int(request_num)

        return {
            "type": message_type,
            "primary_id": fields[1],
            "checkpoint_num": int(fields[2]),
            "my_state": int(fields[3]),
            "high_watermarks": high_watermarks,
        }

    elif message_type == VIEW:
        return {
            "type": message_type,
            "view_num": int(fields[1]),
            "primary_id": fields[2] or None,
            "ready": fields[3].split(",") if fields[3] else [],
            "members": _parse_members(fields[4]),
        }

    elif message_type == REPLICA_HELLO:
        return {
            "type": message_type,
            "replica_id": fields[1],
        }

    elif message_type == REPLICA_STATUS:
        return {
            "type": message_type,
            "replica_id": fields[1],
            "i_am_ready": int(fields[2]),
        }

    raise ValueError(f"Unknown message type: {message_type!r}")


def send_line(sock: socket.socket, line: str) -> None:
    """Send one newline-terminated protocol message."""
    sock.sendall((line + "\n").encode(_ENCODING))


def receive_line(sock: socket.socket, buffer_size: int = 4096) -> str:
    """Receive one newline-terminated message; "" means the peer closed."""
    with _receive_buffers_lock:
        buffer = _receive_buffers.setdefault(sock, bytearray())

    while True:
        newline_index = buffer.find(b"\n")

        if newline_index >= 0:
            line = bytes(buffer[:newline_index])
            del buffer[:newline_index + 1]
            return line.decode(_ENCODING).strip()

        data = sock.recv(buffer_size)

        if not data:
            return ""

        buffer.extend(data)
