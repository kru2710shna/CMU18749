import json

def encode_message(msg_dict: dict) -> bytes:
    """
    Serializes a message dictionary into UTF-8 encoded JSON Lines bytes.
    
    Data processed:
      - msg_dict (dict): Message attributes payload.
      
    Data returned:
      - bytes: Newline-terminated byte stream ready for socket transmission.
    """
    return (json.dumps(msg_dict) + "\n").encode("utf-8")

def parse_stream_buffer(buffer: str):
    """
    Extracts complete newline-delimited JSON messages from a stream buffer.
    
    Data processed:
      - buffer (str): Raw string buffer containing accumulated TCP stream data.
      
    Data returned:
      - tuple (list[dict], str): A list of decoded JSON message dictionaries and 
                                the remaining unparsed buffer fragment.
    """
    lines = buffer.split("\n")
    complete_messages = []
    for line in lines[:-1]:
        if line.strip():
            complete_messages.append(json.loads(line))
    remaining_buffer = lines[-1]
    return complete_messages, remaining_buffer

def build_request(client_id: str, replica_id: str, request_num: int, action: str = "ADD", val: int = 1) -> dict:
    """Constructs a client request payload dictionary."""
    return {
        "msg_type": "REQUEST",
        "client_id": client_id,
        "replica_id": replica_id,
        "request_num": request_num,
        "action": action,
        "val": val
    }

def build_reply(client_id: str, replica_id: str, request_num: int, state: int) -> dict:
    """Constructs a server reply payload dictionary."""
    return {
        "msg_type": "REPLY",
        "client_id": client_id,
        "replica_id": replica_id,
        "request_num": request_num,
        "state": state
    }

def build_heartbeat(sender: str, target: str, heartbeat_count: int) -> dict:
    """Constructs an LFD heartbeat query dictionary."""
    return {
        "msg_type": "HEARTBEAT",
        "sender": sender,
        "target": target,
        "heartbeat_count": heartbeat_count
    }

def build_alive(sender: str, target: str, heartbeat_count: int) -> dict:
    """Constructs a server ALIVE response dictionary."""
    return {
        "msg_type": "ALIVE",
        "sender": sender,
        "target": target,
        "heartbeat_count": heartbeat_count
    }