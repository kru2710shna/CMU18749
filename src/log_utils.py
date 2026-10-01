"""
Shared logging helper used by every process in the system (server, client, LFD, ...).

Why this exists: the assignment requires that *every* console line be prefixed with
a timestamp, and later milestones ask for consistent color-coding across many new
processes (GFD, RM, more LFDs, etc). Centralizing both concerns here means each
process file just calls log(...) and never has to worry about timestamp formatting
or picking colors itself.
"""

from datetime import datetime

# ANSI escape codes for color-coded terminal output. Kept as a simple lookup table
# so new categories (e.g. membership-change, duplicate-detection in M2) can be added
# here without touching any of the process files that call log().
_COLOR_CODES = {
    "reset": "\033[0m",
    "cyan": "\033[36m",     # client <-> server request/reply traffic
    "yellow": "\033[33m",   # heartbeat traffic (LFD <-> server)
    "magenta": "\033[35m",  # state changes (my_state before/after)
    "red": "\033[31m",      # failures / timeouts - should stand out
    "green": "\033[32m",    # successful lifecycle events (startup, registration)
    "white": "\033[37m",    # default / uncategorized messages
}

# Message categories mapped to a color. Every call site picks one of these
# categories instead of a raw color, so the color scheme stays consistent and
# is easy to extend in later milestones (e.g. add "membership" or "duplicate").
CATEGORY_COLORS = {
    "request_reply": "cyan",
    "heartbeat": "yellow",
    "state": "magenta",
    "failure": "red",
    "lifecycle": "green",
    "default": "white",
}


def log(process_label: str, message: str, category: str = "default") -> None:
    """Print a single timestamped, color-coded console line.

    process_label identifies who is printing (e.g. "S1", "C1", "LFD1"), so that
    when everyone's terminal output is compared side by side (or captured into
    one log), it's clear which process produced which line.
    """
    color_name = CATEGORY_COLORS.get(category, "default")
    color_code = _COLOR_CODES[color_name]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{color_code}[{timestamp}] [{process_label}] {message}{_COLOR_CODES['reset']}")
