"""
Shared wire protocol for Milestones 1, 2, and 3.

M1:
    REQUEST|<client_id>|<replica_id>|<request_num>|<op>|<value>
    REPLY|<client_id>|<replica_id>|<request_num>|<op>|<value>
    HEARTBEAT|<lfd_id>|<replica_id>|<heartbeat_count>
    ALIVE|<replica_id>|<lfd_id>|<heartbeat_count>

M2:
    GFD_HELLO|<lfd_id>|<heartbeat_count>
    GFD_ACK|<lfd_id>|<heartbeat_count>
    MEMBER_ADD|<lfd_id>|<replica_id>|<public_host>|<public_port>
    MEMBER_DELETE|<lfd_id>|<replica_id>
    CLIENT_HELLO|<client_id>
    MEMBERSHIP|<replica_id>:<host>:<port>,...

M3:
    CHECKPOINT|<primary_id>|<checkpoint_num>|<my_state>
"""

import socket


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

_DELIMITER = "|"
_ENCODING = "utf-8"


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
) -> str:
    fields = [REPLY, client_id, replica_id, str(request_num), op, str(value)]
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


def build_membership(members) -> str:
    entries = [
        f"{replica_id}:{host}:{port}"
        for replica_id, host, port in members
    ]
    return _DELIMITER.join([MEMBERSHIP, ",".join(entries)])


def build_checkpoint(
    primary_id: str,
    checkpoint_num: int,
    my_state: int,
) -> str:
    """Build an M3 primary-to-backup state checkpoint."""
    fields = [
        CHECKPOINT,
        primary_id,
        str(checkpoint_num),
        str(my_state),
    ]
    return _DELIMITER.join(fields)


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
        members = []

        if fields[1]:
            for entry in fields[1].split(","):
                replica_id, host, port_str = entry.split(":")
                members.append((replica_id, host, int(port_str)))

        return {
            "type": message_type,
            "members": members,
        }

    elif message_type == CHECKPOINT:
        return {
            "type": message_type,
            "primary_id": fields[1],
            "checkpoint_num": int(fields[2]),
            "my_state": int(fields[3]),
        }

    raise ValueError(f"Unknown message type: {message_type!r}")


def send_line(sock: socket.socket, line: str) -> None:
    """Send one newline-terminated protocol message."""
    sock.sendall((line + "\n").encode(_ENCODING))


def receive_line(sock: socket.socket, buffer_size: int = 4096) -> str:
    """Receive one newline-terminated protocol message."""
    data = sock.recv(buffer_size)

    if not data:
        return ""

    return data.decode(_ENCODING).strip()