import socket
import argparse
import time
from log_utils import log, Colors
import protocol

class LocalFaultDetector:
    """
    Represents a Local Fault Detector process.
    
    Data members:
      - lfd_id (str): Unique identifier (e.g., 'LFD1').
      - server_host (str): IP address or hostname of local server replica.
      - server_port (int): TCP port of local server replica.
      - heartbeat_freq (float): Heartbeat period converted to seconds.
      - heartbeat_count (int): Monotonically increasing counter starting at 1.
      - gfd_socket (socket.socket): Connection handle reserved for GFD communication in Milestone 2.
    """
    def __init__(self, lfd_id: str, server_host: str, server_port: int, heartbeat_freq_ms: int):
        self.lfd_id = lfd_id
        self.server_host = server_host
        self.server_port = server_port
        self.heartbeat_freq = heartbeat_freq_ms / 1000.0
        self.heartbeat_count = 1
        self.gfd_socket = None  # Extensible placeholder for Milestone 2 GFD registration

    def run(self):
        """
        Connects to server S1 and enters periodic heartbeat execution loop.
        
        Flow per period:
          1. Transmits HEARTBEAT payload with current heartbeat_count.
          2. Waits for ALIVE response within calculated timeout window.
          3. Increments heartbeat_count on successful response.
          4. Catches socket.timeout, ConnectionResetError, or BrokenPipeError on server crash, 
             logs failure message, and terminates loop.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Timeout configured to twice the heartbeat frequency to allow processing window
        sock.settimeout(self.heartbeat_freq * 2.0)

        try:
            sock.connect((self.server_host, self.server_port))
            log(f"{self.lfd_id} connected to S1 at {self.server_host}:{self.server_port} with frequency {int(self.heartbeat_freq * 1000)}ms", Colors.RESET)
        except Exception as e:
            log(f"{self.lfd_id} failed to connect to S1: {e}", Colors.FAILURE)
            return

        buffer = ""
        try:
            while True:
                hb_msg = protocol.build_heartbeat(self.lfd_id, "S1", self.heartbeat_count)
                log(f"[{self.heartbeat_count}] {self.lfd_id} sending heartbeat to S1", Colors.HEARTBEAT)
                
                sock.sendall(protocol.encode_message(hb_msg))

                # Read response
                received_ack = False
                while not received_ack:
                    data = sock.recv(1024).decode("utf-8")
                    if not data:
                        raise ConnectionResetError("S1 closed connection.")
                    buffer += data
                    messages, buffer = protocol.parse_stream_buffer(buffer)
                    for msg in messages:
                        if msg.get("msg_type") == "ALIVE":
                            log(f"[{self.heartbeat_count}] {self.lfd_id} received heartbeat ACK from S1", Colors.HEARTBEAT)
                            received_ack = True
                            break

                self.heartbeat_count += 1
                time.sleep(self.heartbeat_freq)

        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            log(f"S1 has died. Heartbeat timeout expiration detected at {self.lfd_id}.", Colors.FAILURE)
        except KeyboardInterrupt:
            log(f"{self.lfd_id} terminated manually.", Colors.RESET)
        finally:
            sock.close()

def main():
    parser = argparse.ArgumentParser(description="18-749 Local Fault Detector")
    parser.add_argument("--lfd-id", default="LFD1", help="LFD ID (e.g., LFD1)")
    parser.add_argument("--server-host", default="localhost", help="Target server host")
    parser.add_argument("--server-port", type=int, default=5000, help="Target server port")
    parser.add_argument("--heartbeat-freq-ms", type=int, default=1000, help="Heartbeat period in milliseconds")
    args = parser.parse_args()

    lfd = LocalFaultDetector(args.lfd_id, args.server_host, args.server_port, args.heartbeat_freq_ms)
    lfd.run()

if __name__ == "__main__":
    main()