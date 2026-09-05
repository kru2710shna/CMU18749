"""Shared helpers for timestamped console output.

The course guide asks every process to make its network activity visible in its
own terminal. These helpers keep that output consistent.
"""

from datetime import datetime  # Supplies each process's local timestamp.


def log(process_id: str, text: str) -> None:
    """Print one timestamped message and flush it immediately for the live demo."""

    # This is a local timestamp for debugging, not a global distributed-system clock.
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # flush=True ensures terminal output appears immediately during the demonstration.
    print(f"[{timestamp}] {process_id}: {text}", flush=True)


def request_label(client_id: str, replica_id: str, request_num: int, direction: str) -> str:
    """Return the tuple notation requested by the project guide."""

    # Example result: <C1, S1, 7, request>.
    return f"<{client_id}, {replica_id}, {request_num}, {direction}>"
