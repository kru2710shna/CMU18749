import socket
import selectors
import argparse
import types
from log_utils import log, Colors
import protocol

class ServerReplica:
    """
    Represents a stateful server replica process.
    
    Data members:
      - replica_id (str): Unique server identifier (e.g., 'S1').
      - host (str): Host interface address to bind to.
      - port (int): TCP port to listen on.
      - my_state (int): Stateful shared variable (counter starting at 0).
      - sel (selectors.DefaultSelector): Non-blocking I/O multiplexer.
      - i_am_ready (int): Readiness flag reserved for future milestone state recovery.
      - is_primary (bool): Replication role flag reserved for future milestones.
      - checkpoint_count (int): Counter reserved for passive replication checkpoints.
    """
    def __init__(self, replica_id: str, host: str, port: int):
        self.replica_id = replica_id
        self.host = host
        self.port = port
        self.my_state = 0
        self.sel = selectors.DefaultSelector()
        
        # State placeholders for future milestone compatibility
        self.i_am_ready = 1
        self.is_primary = True
        self.checkpoint_count = 0

    def start(self):
        """Configures listening TCP socket with SO_REUSEADDR and registers selector event listener."""
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, self.port))
        lsock.listen()
        lsock.setblocking(False)
        self.sel.register(lsock, selectors.EVENT_READ, data=None)
        log(f"Server {self.replica_id} listening on {self.host}:{self.port} (Initial state: my_state = {self.my_state})", Colors.GREEN)

    def accept_connection(self, sock: socket.socket):
        """Accepts new incoming client/LFD connection and associates per-connection buffer data."""
        conn, addr = sock.accept()
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, buffer="")
        self.sel.register(conn, selectors.EVENT_READ, data=data)

    def read_connection(self, key, mask):
        """Reads incoming socket data, appends bytes to connection buffer, and processes parsed messages."""
        conn = key.fileobj
        data = key.data
        try:
            recv_bytes = conn.recv(1024)
            if recv_bytes:
                data.buffer += recv_bytes.decode("utf-8")
                messages, data.buffer = protocol.parse_stream_buffer(data.buffer)
                for msg in messages:
                    self.process_message(conn, msg)
            else:
                self.sel.unregister(conn)
                conn.close()
        except (ConnectionResetError, BrokenPipeError):
            self.sel.unregister(conn)
            conn.close()

    def process_message(self, conn: socket.socket, msg: dict):
        """
        Parses JSON message type, executes state transitions, logs output, and transmits response.
        
        Data processed:
          - conn (socket.socket): Connection handle sending the message.
          - msg (dict): Deserialized message schema dictionary.
          
        Processing rules:
          - REQUEST: Logs <C, S, num, request>, outputs state before update, applies update, 
                     outputs state after update, and transmits REPLY payload.
          - HEARTBEAT: Logs heartbeat receipt and transmits ALIVE payload.
        """
        msg_type = msg.get("msg_type")

        if msg_type == "REQUEST":
            client_id = msg["client_id"]
            req_num = msg["request_num"]
            action = msg.get("action", "ADD")
            val = msg.get("val", 1)

            log(f"Received <{client_id}, {self.replica_id}, {req_num}, request>", Colors.GREEN)
            log(f"my_state_{self.replica_id} = {self.my_state} before processing <{client_id}, {self.replica_id}, {req_num}, request>", Colors.GREEN)

            # Apply state mutation
            if action == "ADD":
                self.my_state += val

            log(f"my_state_{self.replica_id} = {self.my_state} after processing <{client_id}, {self.replica_id}, {req_num}, request>", Colors.GREEN)

            reply = protocol.build_reply(client_id, self.replica_id, req_num, self.my_state)
            log(f"Sending <{client_id}, {self.replica_id}, {req_num}, reply>", Colors.GREEN)
            conn.sendall(protocol.encode_message(reply))

        elif msg_type == "HEARTBEAT":
            sender = msg["sender"]
            hb_count = msg["heartbeat_count"]
            log(f"[{hb_count}] {self.replica_id} receives heartbeat from {sender}", Colors.GREEN)
            
            alive_reply = protocol.build_alive(self.replica_id, sender, hb_count)
            log(f"[{hb_count}] {self.replica_id} sending heartbeat ACK to {sender}", Colors.GREEN)
            conn.sendall(protocol.encode_message(alive_reply))

    def run(self):
        """Executes infinite event loop monitoring socket readiness."""
        self.start()
        try:
            while True:
                events = self.sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        self.accept_connection(key.fileobj)
                    else:
                        self.read_connection(key, mask)
        except KeyboardInterrupt:
            log(f"Server {self.replica_id} shutting down via Ctrl-C.", Colors.RED)
        finally:
            self.sel.close()

def main():
    parser = argparse.ArgumentParser(description="18-749 Stateful Server Replica")
    parser.add_argument("--replica-id", default="S1", help="Replica ID (e.g., S1)")
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind")
    parser.add_argument("--port", type=int, default=5000, help="Listening TCP port")
    args = parser.parse_args()

    server = ServerReplica(args.replica_id, args.host, args.port)
    server.run()

if __name__ == "__main__":
    main()