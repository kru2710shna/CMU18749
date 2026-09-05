"""Small JSON-over-TCP helpers used by every Milestone 1 process.

Each TCP connection carries exactly one JSON object followed by a newline.  Keeping
one message per connection makes the first milestone easy to understand and keeps
the single-threaded server deterministic.
"""

import json  # Converts Python dictionaries to and from JSON text.
import socket  # Provides the TCP socket type used by clients, S1, and LFD1.
from typing import Any, Dict  # Documents the shape of messages in this file.


class ProtocolError(ValueError):
    """Raised when a peer sends an invalid or incomplete protocol message."""


def send_message(connection: socket.socket, message: Dict[str, Any]) -> None:
    """Serialize one dictionary and send it as one newline-delimited JSON message."""

    # Compact JSON makes terminal/network messages short while still being readable.
    encoded_text = json.dumps(message, separators=(",", ":")) + "\n"

    # TCP sends bytes, so convert the text to UTF-8 bytes before sending it.
    connection.sendall(encoded_text.encode("utf-8"))


def receive_message(connection: socket.socket) -> Dict[str, Any]:
    """Read one newline-delimited JSON message from a TCP connection."""

    # Accumulate bytes because a TCP message can arrive in more than one recv call.
    buffer = bytearray()

    while True:
        # Read up to 4096 bytes from the peer.
        chunk = connection.recv(4096)

        # An empty byte string means the peer closed the connection too early.
        if not chunk:
            raise ProtocolError("peer closed the connection before sending a message")

        # Add the newly received bytes to the message buffer.
        buffer.extend(chunk)

        # A newline marks the end of the one message this connection should carry.
        if b"\n" in buffer:
            break

    # Keep only the first line; Milestone 1 intentionally uses one message per socket.
    message_bytes = bytes(buffer).split(b"\n", maxsplit=1)[0]

    try:
        # Decode UTF-8 bytes, parse JSON, and obtain a Python dictionary.
        message = json.loads(message_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        # Convert parsing details into one project-specific error for callers.
        raise ProtocolError(f"invalid JSON message: {error}") from error

    # Every valid protocol message must be a JSON object, not a list/string/number.
    if not isinstance(message, dict):
        raise ProtocolError("protocol message must be a JSON object")

    return message


def require_fields(message: Dict[str, Any], *field_names: str) -> None:
    """Ensure that a received message contains every field needed by its handler."""

    # Find fields that are absent before using them and causing a less clear KeyError.
    missing = [field_name for field_name in field_names if field_name not in message]

    # Stop handling this message with a useful explanation if a required field is missing.
    if missing:
        raise ProtocolError(f"message is missing required field(s): {', '.join(missing)}")
