import datetime

class Colors:
    REQUEST = "\033[96m"    # Cyan: Request messages (C -> S)
    REPLY = "\033[92m"      # Green: Reply messages (S -> C)
    STATE = "\033[95m"      # Magenta: State mutation logs (my_state)
    HEARTBEAT = "\033[93m"  # Yellow: Heartbeat & ALIVE messages (LFD <-> S)
    FAILURE = "\033[91m"    # Red: Timeout expirations & server crashes
    RESET = "\033[0m"

def log(msg: str, color: str = Colors.RESET):
    """
    Prints a timestamped message to standard output using event-based coloring.
    
    Data processed:
      - msg (str): Text message to be logged.
      - color (str): ANSI escape code string representing the specific message type.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{color}[{timestamp}] {msg}{Colors.RESET}", flush=True)