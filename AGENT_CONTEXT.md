# Milestone 1 — Implementation Context for AI Coding Agents

This file summarizes the current state of the Milestone 1 implementation so any
teammate's AI coding assistant can pick up context quickly without re-reading
the whole conversation history. It reflects what has been **built and tested**,
not just planned.

## Status: Milestone 1 is complete and verified

All 9 rubric items (server, LFD, 3 clients, stateful behavior, timestamped
logging, message-tuple identification, heartbeat failure detection on Ctrl-C,
configurable `heartbeat_freq`) have been implemented and manually tested,
including a real single-server-crash / multi-laptop demo.

## Project structure

```
project/
  src/
    server_replica.py         # S1: owns my_state, handles client requests + LFD heartbeats
    client.py                 # C1/C2/C3 (same file, different --client-id)
    local_fault_detector.py   # LFD1: heartbeats S1, detects timeout/crash
    protocol.py               # message format (build/parse) + send/recv helpers
    net_utils.py              # generic TCP server/client socket helpers
    log_utils.py              # timestamped, color-coded console logging
  run_demo.sh                 # tmux script: launches S1+LFD1+C1+C2+C3 in one window
  README.md                   # milestone spec/checklist (already existed)
  The-Project.txt             # full semester project spec (already existed)
```

## Design decisions worth knowing

- **Wire protocol** (`protocol.py`): pipe-delimited text lines, newline-terminated.
  ```
  REQUEST|<client_id>|<replica_id>|<request_num>|<op>|<value>
  REPLY|<client_id>|<replica_id>|<request_num>|<op>|<value>
  HEARTBEAT|<lfd_id>|<replica_id>|<heartbeat_count>
  ALIVE|<replica_id>|<lfd_id>|<heartbeat_count>
  ```
  `parse_message()` returns a dict keyed by field name. This is intentionally
  generic/centralized so M2+ can add new message types (CHECKPOINT, membership
  changes) without touching call sites elsewhere.

- **Socket helpers** (`net_utils.py`): two reusable patterns —
  `start_tcp_server(host, port, handle_connection)` (spawns a thread per
  accepted connection) and `connect_to_server(host, port)`. Every process uses
  these instead of hand-rolling socket code, since M2 triples connections per
  replica and M3 adds a checkpoint channel.

- **Server concurrency** (`server_replica.py`): each TCP connection (from a
  client or from the LFD) gets its own thread so slow peers don't block others
  from connecting, but all reads/writes of `my_state` are behind one
  `threading.Lock`. This preserves the "server is deterministic / effectively
  single-threaded state machine" assumption in the spec while still allowing
  concurrent connections.

- **LFD timeout** (`local_fault_detector.py`): timeout = `heartbeat_freq * 2`.
  `--heartbeat-freq` is a **required** CLI arg with no default, per the "must
  not be hardcoded" requirement. On timeout/disconnect it logs a red
  `FAILED/TIMED OUT ... presumed crashed` message, then keeps retrying to
  reconnect (so relaunching S1 later is picked back up automatically — useful
  going into M4/M5 recovery, though not required for M1).

- **Client modes** (`client.py`): `--mode auto` (default) sends `ADD 1` on a
  timer forever, `--mode manual` prompts the operator for an ADD amount before
  each request (Enter = default, `q` = quit). Both are explicitly allowed by
  the spec ("manual client requests are allowed... continuous loop is
  optional but useful").

- **Logging** (`log_utils.py`): every line is timestamped
  (`[YYYY-MM-DD HH:MM:SS.mmm]`) and color-coded by category:
  - cyan = client/server request-reply traffic
  - yellow = heartbeat traffic
  - magenta = state changes (`my_state` before/after)
  - green = lifecycle events (startup, connect)
  - red = failures/timeouts
  Categories are a lookup table (`CATEGORY_COLORS`) so new categories
  (membership-change, duplicate-detection in M2) can be added without
  touching call sites.

## Bugs found and fixed during testing

1. **Unhandled `ConnectionResetError` in client** — when S1 was killed
   mid-request, `sock.recv()` sometimes raised `ConnectionResetError`
   (`Errno 54`) instead of returning empty bytes (clean close), which crashed
   the client with a raw traceback. Fixed by wrapping the client's
   send/receive in `try/except OSError`, so both failure modes now print a
   clean red "Connection to S1 failed/closed" message and exit gracefully.
2. **Same class of bug at client startup** — connecting to a server that
   isn't up yet raised an uncaught `ConnectionRefusedError`. Fixed the same
   way in `run_client()`'s initial `connect_to_server()` call.
3. **Manual-mode input crash** — a stray terminal escape-sequence byte
   (observed as a literal `]` character leaking into stdin on one teammate's
   terminal) caused `int(raw_input_value)` to raise `ValueError` and crash the
   client. Fixed by wrapping the parse in a retry loop that re-prompts on
   invalid input instead of crashing.

## Demo tooling

`run_demo.sh` (repo root) launches S1, LFD1, C1, C2, C3 as 5 tmux panes in one
window/screen, so the whole system is visible at once instead of juggling 5
terminal windows. Each pane is still a real independent process (Ctrl-C in the
S1 pane kills only S1, matching the rubric's manual fault-injection step).

```
./run_demo.sh            # heartbeat_freq defaults to 1 second
./run_demo.sh 0.5        # re-demo with a different heartbeat_freq
```

Requires `tmux` (`brew install tmux`). Mouse mode is enabled automatically so
panes can be focused by clicking (more reliable than the `Ctrl-B` + arrow-key
prefix across different terminal apps/keyboard layouts).

To end a demo: `Ctrl-B` then `D` to detach (keeps running), or
`tmux kill-session -t m1-demo` to fully stop everything.

## Multi-machine (2-laptop) demo

Server machine runs S1 + LFD1 (both against `localhost`); client machine runs
C1/C2/C3 pointed at the server machine's LAN IP:

```
# server machine
python3 server_replica.py --replica-id S1 --host 0.0.0.0 --port 5000
python3 local_fault_detector.py --lfd-id LFD1 --replica-id S1 --server-host localhost --server-port 5000 --heartbeat-freq 1

# client machine (repeat for C1, C2, C3)
python3 client.py --client-id C1 --server-host <SERVER_LAN_IP> --server-port 5000
```

Get the server machine's LAN IP with `ipconfig getifaddr en0` (macOS Wi-Fi).
Both machines must be on the same network; some shared/campus Wi-Fi blocks
device-to-device traffic — a personal hotspot is a reliable fallback.
`run_demo.sh` is single-laptop only (hardcodes `localhost`); for two machines,
run the commands manually as above.

## Explicitly out of scope for M1 (do not add yet)

Per the spec and confirmed in review: no replication, no GFD/RM, no duplicate
detection, no checkpointing, no automated recovery/reconnect logic beyond what
LFD1 already does for its own convenience. These are M2–M5 concerns and adding
them now would be scope creep against the M1 rubric.
