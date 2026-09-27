# Milestones 1 & 2 — Implementation Context for AI Coding Agents

This file summarizes the current state of the implementation so any
teammate's AI coding assistant can pick up context quickly without re-reading
the whole conversation history. It reflects what has been **built and tested**,
not just planned. Milestone 1 is documented first below (unchanged); Milestone
2 additions are in their own section further down.

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

---

## Status: Milestone 2 is complete and verified

All rubric items (GFD startup/membership printing, LFD↔GFD registration and
heartbeating, membership add/delete on replica up/down, 3-way client fanout,
duplicate-reply detection, continued operation after a single replica is
killed) have been implemented and verified end-to-end on one laptop (GFD + 3×
(replica+LFD) + 3 clients, ports 6000/5001/5002/5003), including killing S1
mid-demo and confirming the client kept going on S2/S3 alone while the GFD's
membership count dropped to 2.

## Guiding constraint for M2: additive only

The user was explicit: **do not change the existing M1 architecture, code, or
design - only build on top of it.** Concretely, that meant:

- `server_replica.py` is **byte-for-byte unchanged**. Active replication needs
  zero coordination between replicas (each is deterministic and processes the
  same inputs independently), so the exact same M1 file just runs 3 times as
  S1/S2/S3. This is also explicitly sanctioned by the assignment's own Tips &
  Tricks ("You can use the same server code for both active replication and
  passive replication").
- `local_fault_detector.py` and `client.py` gained new, **opt-in** code paths
  (`--gfd-host`/`--gfd-port` on the LFD; `--replicas` on the client) rather
  than being rewritten. Omitting the new flags reproduces exact Milestone 1
  behavior - verified directly (see "Bugs found and fixed" below has no new
  entries from this regression check; a live side-by-side run of
  `local_fault_detector.py`/`client.py` with and without the new flags showed
  byte-identical M1-mode log output).
- `protocol.py` only gained new `build_*`/`parse_message` branches
  (`GFD_HELLO`/`GFD_ACK`/`MEMBER_ADD`/`MEMBER_DELETE`); every M1 message type
  is untouched. This was already the file's own stated design intent (see its
  module docstring from M1: "adding a new field or message type later means
  editing this file only").
- `log_utils.py` only gained one new category (`membership`/`duplicate` →
  blue); all M1 categories/colors are untouched.
- `run_demo.sh` (M1) was left completely alone. A new `run_demo_m2.sh` handles
  the M2 topology instead of overloading the M1 script with mode flags.

## Design decisions worth knowing

- **LFD↔GFD wire messages** (`protocol.py`): `GFD_HELLO`/`GFD_ACK` mirror the
  existing `HEARTBEAT`/`ALIVE` pair exactly (same register-then-heartbeat
  pattern, one level up the hierarchy - this was anticipated in the M1
  `local_fault_detector.py` docstring). `MEMBER_ADD`/`MEMBER_DELETE` carry
  just `(lfd_id, replica_id)`.
- **One connection per LFD→GFD, multiplexed** (`local_fault_detector.py`):
  per the assignment ("1 TCP/IP connection to the GFD"), the LFD opens exactly
  one persistent socket to the GFD at startup. Two things write to it
  concurrently: a dedicated heartbeat thread (`run_gfd_heartbeat_loop`) and,
  on a replica-state transition, the (unchanged-in-logic) replica-heartbeat
  loop. A small `GFDLink` wrapper class serializes writes behind a lock; only
  the heartbeat thread ever reads from the socket, so reads need no locking.
- **Membership add/delete fire exactly once per transition, not every
  heartbeat**: a `registered_with_gfd` flag (per LFD run) tracks whether the
  GFD currently believes the replica is up; `MEMBER_ADD` fires only on a
  down/unknown→up transition, `MEMBER_DELETE` only on up→down. This matches
  the rubric's "after the first successful heartbeat" wording exactly.
- **LFD→GFD heartbeat frequency reuses `--heartbeat-freq`** (the same value
  already used for LFD→replica) rather than introducing a second CLI flag.
  The assignment's rubric for M2 never tests independent GFD heartbeat
  tuning - that's listed only as a later "dynamic reconfiguration" property,
  out of scope here per the user's instructions to stay within M2.
- **GFD membership is an ordered list, not a set** (`gfd.py`): printed order
  matches join order (e.g. "GFD: 2 members: S1, S2"), matching every example
  in the assignment PDF. Guarded by a `threading.Lock` since multiple LFD
  connections (one thread each, via `net_utils.start_tcp_server`) can send
  `MEMBER_ADD`/`MEMBER_DELETE` concurrently.
- **Client fanout is round-based with a bounded per-reply wait**
  (`client.py`, `run_client_multireplica`): for `request_num` N, the client
  sends to all live replicas back-to-back *before* reading any reply back
  (matching the assignment's notation example ordering), then reads whichever
  replies arrive within `--reply-timeout-ms` per replica, delivers the first,
  and logs `"request_num N: Discarded duplicate reply from Sx"` for the rest.
  This was a deliberate simplicity/fidelity trade-off - discussed explicitly
  with the user - against an "advance to N+1 as soon as the first reply
  arrives" model that would need a persistent background reader thread per
  replica connection (to catch a slow reply arriving after the client has
  already moved on to a later round). The round-based model is simpler,
  single-threaded, and consistent with the blocking/sequential style already
  used everywhere else in this codebase; it fully satisfies every M2 rubric
  item and "what I will look for" bullet in the assignment PDF.
- **A replica marked "down" by a client stays down for that client's lifetime
  ** (no reconnect attempts). The assignment's M2 "what I will not look for"
  section explicitly excludes automated recovery, so there was no reason to
  add reconnect complexity to the client (unlike the LFD's own replica
  connection, which *does* reconnect, because recovery scenarios are tested
  starting at M4).
- **Ports used for the single-laptop M2 demo** (`run_demo_m2.sh`): GFD=6000,
  S1=5001, S2=5002, S3=5003 (deliberately off 5000, since macOS's AirPlay
  Receiver squats on 5000 by default - see the M1 bug notes below).

## Demo tooling (M2)

`run_demo_m2.sh` (repo root) launches GFD + (S1,LFD1) + (S2,LFD2) + (S3,LFD3)
+ C1 + C2 + C3 as 10 tmux panes in one window, all on `localhost`:

```
./run_demo_m2.sh          # heartbeat_freq defaults to 1 second
./run_demo_m2.sh 0.5      # re-demo with a different heartbeat_freq
```

For the real multi-machine demo, see the "Running Milestone 2" section of
[README.md](README.md) - GFD runs on the sacred/client machine, each
replica+LFD pair runs on its own non-sacred machine, and the client's
`--replicas` flag takes a comma-separated `replica_id:host:port` list.

## What was verified live for M2

Using the ports above on one laptop: GFD started and printed `GFD: 0
members`; S1/S2/S3 + LFD1/2/3 came up and GFD's membership grew to `GFD: 3
members: ...` as each LFD's first heartbeat to its replica succeeded; a
client with `--replicas` connected to all three, sent request_num 1-3 to all
three back-to-back, and correctly delivered exactly one reply per round while
discarding the other two as duplicates. Then S1 was `kill -9`'d mid-run: the
client's very next round got a `Connection reset by peer` from S1 (logged,
S1 marked down), continued delivering from S2 with S3's reply discarded as a
duplicate, and never paused. Simultaneously, LFD1 logged `Heartbeat to S1
FAILED/TIMED OUT ... Replica presumed crashed`, sent `LFD1: delete replica
S1` to the GFD, and the GFD printed `GFD: 2 members: S2, S3` - all exactly
matching the rubric's expected console text.

## Explicitly out of scope for M2 (do not add yet)

Per the spec and the user's explicit instruction to stop at M2: no Replication
Manager, no passive replication, no checkpointing, no automated or manual
recovery/relaunch of a dead replica, no multi-fault scenarios beyond "kill one
replica, then optionally kill a second after some time has passed." These are
M3-M5 concerns.
