import datetime

class Colors:
    BLUE = "\033[94m"    # Client output
    GREEN = "\033[92m"   # Server output
    YELLOW = "\033[93m"  # LFD output
    RED = "\033[91m"     # Failure / Timeout alerts
    RESET = "\033[0m"

def log(msg: str, color: str = Colors.RESET):
    """
    Prints a timestamped message to standard output.
    
    Data processed:
      - msg (str): Text message to be logged.
      - color (str): ANSI escape code string for log coloring.
      
    Data returned:
      - None (outputs directly to console with format: [YYYY-MM-DD HH:MM:SS.mmm] <msg>).
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{color}[{timestamp}] {msg}{Colors.RESET}", flush=True)