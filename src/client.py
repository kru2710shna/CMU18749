import socket
import argparse
import time
from log_utils import log, Colors
import protocol

class Client:
    """
    Represents an independent client process.
    
    Data members:
      - client_id (str): Unique identifier (e.g., 'C1').
      - server_host (str): IP address or hostname of the target server.
      - server_port (int): TCP port of the target server.
      - request_num (int): Monotonically increasing sequence number starting at 1.
      - server_sockets (dict): Mapping of server IDs to socket connections.
    """
    def __init__(self, client_id: str, server_host: str, server_port: int):
        self.client_id = client_id
        self.server_host = server_host
        self.server_port = server_port
        self.request_num = 1
        self.server_sockets = {}  # Extensible for multiple replicas in future milestones

    def get_or_connect_socket(self, target_replica: str = "S1"):
        """Returns existing socket or attempts to establish a new TCP connection."""
        if target_replica in self.server_sockets:
            return self.server_sockets[target_replica]
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((self.server_host, self.server_port))
            self.server_sockets[target_replica] = sock
            log(f"Client {self.client_id} connected to {target_replica}.", Colors.RESET)
            return sock
        except (ConnectionError, OSError):
            return None

    def send_request(self, target_replica: str = "S1", action: str = "ADD", val: int = 1):
        """
        Constructs, sends, and waits for a reply to a single client request.
        
        Data processed:
          - target_replica (str): Destination server replica ID.
          - action (str): Operation to execute on state (e.g., 'ADD').
          - val (int): Value argument for operation.
          
        Flow:
          1. Builds request message and logs outgoing request tuple.
          2. Encodes and transmits JSON Line payload over TCP socket.
          3. Reads from socket until newline delimiter is reached.
          4. Decodes JSON reply, logs incoming reply tuple, and increments request_num.
        """
        sock = self.get_or_connect_socket(target_replica)
        if not sock:
            log(f"<{self.client_id}, {target_replica}, {self.request_num}, request failed: {target_replica} unavailable>", Colors.FAILURE)
            return False

        req_msg = protocol.build_request(self.client_id, target_replica, self.request_num, action, val)
        log(f"Sent <{self.client_id}, {target_replica}, {self.request_num}, request>", Colors.REQUEST)

        try:
            sock.sendall(protocol.encode_message(req_msg))

            # Receive reply (blocking until newline framing complete)
            buffer = ""
            while True:
                data = sock.recv(1024).decode("utf-8")
                if not data:
                    raise ConnectionError("Server closed connection.")
                buffer += data
                messages, buffer = protocol.parse_stream_buffer(buffer)
                if messages:
                    reply_msg = messages[0]
                    break

            log(f"Received <{reply_msg['client_id']}, {reply_msg['replica_id']}, {reply_msg['request_num']}, reply>", Colors.REPLY)
            self.request_num += 1
            return True

        except (socket.timeout, ConnectionError, OSError):
            log(f"<{self.client_id}, {target_replica}, {self.request_num}, request failed: Connection lost>", Colors.FAILURE)
            sock.close()
            if target_replica in self.server_sockets:
                del self.server_sockets[target_replica]
            return False

    def run_interactive_or_loop(self, loop: bool = False, delay: float = 2.0):
        """
        Executes request workflow either in a continuous loop or manually via console prompt.
        
        Data processed:
          - loop (bool): Enable continuous request transmission.
          - delay (float): Interval in seconds between requests in loop mode.
        """
        try:
            if loop:
                log(f"Starting continuous request loop with delay {delay}s...", Colors.RESET)
                while True:
                    self.send_request("S1", "ADD", 1)
                    time.sleep(delay)
            else:
                while True:
                    user_input = input("Press ENTER to send request (or type 'q' to quit): ")
                    if user_input.lower() == 'q':
                        break
                    self.send_request("S1", "ADD", 1)
        except KeyboardInterrupt:
            log(f"Client {self.client_id} disconnected.", Colors.RESET)
        finally:
            for sock in list(self.server_sockets.values()):
                sock.close()

def main():
    parser = argparse.ArgumentParser(description="18-749 Client Process")
    parser.add_argument("--client-id", required=True, help="Unique identifier for client (e.g., C1)")
    parser.add_argument("--server-host", default="localhost", help="Server hostname/IP")
    parser.add_argument("--server-port", type=int, default=5000, help="Server TCP port")
    parser.add_argument("--loop", action="store_true", help="Run automated continuous request loop")
    parser.add_argument("--delay", type=float, default=2.0, help="Loop delay in seconds")
    args = parser.parse_args()

    client = Client(args.client_id, args.server_host, args.server_port)
    client.run_interactive_or_loop(loop=args.loop, delay=args.delay)

if __name__ == "__main__":
    main()