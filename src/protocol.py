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

Milestone 2 adds the LFD<->GFD channel and membership-change notices,
following the same "one shared set of functions" pattern:
    GFD_HELLO|<lfd_id>|<heartbeat_count>
    GFD_ACK|<lfd_id>|<heartbeat_count>
    MEMBER_ADD|<lfd_id>|<replica_id>|<public_host>|<public_port>
    MEMBER_DELETE|<lfd_id>|<replica_id>

The updated Milestone 2 spec additionally requires clients to learn
membership from the GFD (rather than being told a static replica list on
the command line), and for the GFD to push every membership change to
every connected client. Same pattern again, one level of the hierarchy
further out:
    CLIENT_HELLO|<client_id>
    MEMBERSHIP|<replica_id>:<host>:<port>,<replica_id>:<host>:<port>,...
"""

import socket

# Message type tags used as the first field of every line.
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


def build_gfd_hello(lfd_id: str, heartbeat_count: int) -> str:
    """Build a GFD_HELLO line: an LFD's heartbeat to the GFD (Milestone 2).

    One connection serves both this heartbeat and the membership-change
    messages below, per the assignment's "1 TCP/IP connection to the GFD"
    per LFD.
    """
    fields = [GFD_HELLO, lfd_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_gfd_ack(lfd_id: str, heartbeat_count: int) -> str:
    """Build a GFD_ACK line: the GFD's response to a GFD_HELLO heartbeat."""
    fields = [GFD_ACK, lfd_id, str(heartbeat_count)]
    return _DELIMITER.join(fields)


def build_member_add(lfd_id: str, replica_id: str, public_host: str, public_port: int) -> str:
    """Build a MEMBER_ADD line: an LFD telling the GFD its replica is healthy.

    Sent once, the first time an LFD's heartbeat to its replica succeeds
    (at startup, or after the replica has been manually relaunched).
    Carries the replica's publicly-reachable host/port (distinct from the
    LFD's own --server-host, which is normally "localhost" since the LFD
    and its replica are co-located) so the GFD can relay it on to clients,
    which run on a different, sacred machine.
    """
    fields = [MEMBER_ADD, lfd_id, replica_id, public_host, str(public_port)]
    return _DELIMITER.join(fields)


def build_member_delete(lfd_id: str, replica_id: str) -> str:
    """Build a MEMBER_DELETE line: an LFD telling the GFD its replica died.

    Sent once, the moment an LFD's heartbeat to its replica times out or
    the connection drops.
    """
    fields = [MEMBER_DELETE, lfd_id, replica_id]
    return _DELIMITER.join(fields)


def build_client_hello(client_id: str) -> str:
    """Build a CLIENT_HELLO line: a client registering with the GFD so it
    can learn (and be kept up to date on) the current replica membership."""
    fields = [CLIENT_HELLO, client_id]
    return _DELIMITER.join(fields)


def build_membership(members) -> str:
    """Build a MEMBERSHIP line: the GFD's full current-membership snapshot,
    sent to a client once at registration and again on every subsequent
    membership change. `members` is an iterable of (replica_id, host, port).

    Always the *full* snapshot (not an incremental add/delete) so a client
    that missed an earlier update - or that just connected - can't drift
    out of sync with the GFD's view of the world.
    """
    entries = [f"{replica_id}:{host}:{port}" for replica_id, host, port in members]
    return _DELIMITER.join([MEMBERSHIP, ",".join(entries)])


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
