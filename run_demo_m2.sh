#!/usr/bin/env bash
#
# Launches the full Milestone 2 demo (GFD, S1/LFD1, S2/LFD2, S3/LFD3, C1, C2, C3)
# in one tmux session, all on localhost, for single-laptop testing before a
# real multi-machine run. run_demo.sh (Milestone 1) is left untouched; this is
# a separate script for the Milestone 2 topology.
#
# Uses 5 tmux WINDOWS (tabs) instead of cramming 10 panes into one window, so
# it fits on any terminal size - no window ever needs more than a 3-way split:
#   window 0 "gfd"     - GFD alone
#   window 1 "s1"      - S1 | LFD1
#   window 2 "s2"      - S2 | LFD2
#   window 3 "s3"      - S3 | LFD3
#   window 4 "clients" - C1 | C2 | C3
#
# Switch windows with Ctrl-B then a number (e.g. Ctrl-B 1 for the s1 window),
# Ctrl-B w to pick from a list, or just click a window's name in the status
# bar at the bottom (mouse mode is on). Switch panes within a window by
# clicking them, or Ctrl-B then an arrow key.
#
# Ports used: GFD=6000, S1=5001, S2=5002, S3=5003 (kept off 5000 in case
# something else on the machine - e.g. macOS's AirPlay Receiver - is already
# squatting on it).
#
# Usage:
#   ./run_demo_m2.sh [heartbeat_freq_seconds]
#
# Example:
#   ./run_demo_m2.sh 1      # LFD1/2/3 heartbeat their replicas AND the GFD every 1 second
#   ./run_demo_m2.sh 0.5    # restart with a different heartbeat_freq

set -euo pipefail

HEARTBEAT_FREQ="${1:-1}"
SESSION_NAME="m2-demo"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/src" && pwd)"
GFD_PORT=6000
S1_PORT=5001
S2_PORT=5002
S3_PORT=5003
REPLICAS="S1:localhost:$S1_PORT,S2:localhost:$S2_PORT,S3:localhost:$S3_PORT"

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install it with 'brew install tmux' and re-run this script."
    exit 1
fi

# Kill any leftover session from a previous demo run so panes don't stack up.
tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true

tmux new-session -d -s "$SESSION_NAME" -n gfd -c "$SRC_DIR"
tmux set -g mouse on

tmux send-keys -t "$SESSION_NAME:gfd" \
    "python3 gfd.py --host 0.0.0.0 --port $GFD_PORT" C-m
sleep 0.5

# --- window 1: S1 | LFD1 ---
tmux new-window -t "$SESSION_NAME" -n s1 -c "$SRC_DIR"
tmux split-window -h -t "$SESSION_NAME:s1" -c "$SRC_DIR"
tmux send-keys -t "$SESSION_NAME:s1.0" \
    "python3 server_replica.py --replica-id S1 --host 0.0.0.0 --port $S1_PORT" C-m

# --- window 2: S2 | LFD2 ---
tmux new-window -t "$SESSION_NAME" -n s2 -c "$SRC_DIR"
tmux split-window -h -t "$SESSION_NAME:s2" -c "$SRC_DIR"
tmux send-keys -t "$SESSION_NAME:s2.0" \
    "python3 server_replica.py --replica-id S2 --host 0.0.0.0 --port $S2_PORT" C-m

# --- window 3: S3 | LFD3 ---
tmux new-window -t "$SESSION_NAME" -n s3 -c "$SRC_DIR"
tmux split-window -h -t "$SESSION_NAME:s3" -c "$SRC_DIR"
tmux send-keys -t "$SESSION_NAME:s3.0" \
    "python3 server_replica.py --replica-id S3 --host 0.0.0.0 --port $S3_PORT" C-m
sleep 0.5

# LFDs started after their replicas, so the very first heartbeat has
# something to connect to.
tmux send-keys -t "$SESSION_NAME:s1.1" \
    "python3 local_fault_detector.py --lfd-id LFD1 --replica-id S1 --server-host localhost --server-port $S1_PORT --heartbeat-freq $HEARTBEAT_FREQ --gfd-host localhost --gfd-port $GFD_PORT" C-m
tmux send-keys -t "$SESSION_NAME:s2.1" \
    "python3 local_fault_detector.py --lfd-id LFD2 --replica-id S2 --server-host localhost --server-port $S2_PORT --heartbeat-freq $HEARTBEAT_FREQ --gfd-host localhost --gfd-port $GFD_PORT" C-m
tmux send-keys -t "$SESSION_NAME:s3.1" \
    "python3 local_fault_detector.py --lfd-id LFD3 --replica-id S3 --server-host localhost --server-port $S3_PORT --heartbeat-freq $HEARTBEAT_FREQ --gfd-host localhost --gfd-port $GFD_PORT" C-m
sleep 1

# --- window 4: C1 | C2 | C3 ---
tmux new-window -t "$SESSION_NAME" -n clients -c "$SRC_DIR"
tmux split-window -h -t "$SESSION_NAME:clients" -c "$SRC_DIR"
tmux split-window -h -t "$SESSION_NAME:clients" -c "$SRC_DIR"
tmux select-layout -t "$SESSION_NAME:clients" even-horizontal
tmux send-keys -t "$SESSION_NAME:clients.0" \
    "python3 client.py --client-id C1 --replicas $REPLICAS" C-m
tmux send-keys -t "$SESSION_NAME:clients.1" \
    "python3 client.py --client-id C2 --replicas $REPLICAS" C-m
tmux send-keys -t "$SESSION_NAME:clients.2" \
    "python3 client.py --client-id C3 --replicas $REPLICAS" C-m

tmux select-window -t "$SESSION_NAME:gfd"

echo "M2 demo running in tmux session '$SESSION_NAME' with heartbeat_freq=$HEARTBEAT_FREQ."
echo "Windows: 0=gfd  1=s1 (S1|LFD1)  2=s2 (S2|LFD2)  3=s3 (S3|LFD3)  4=clients (C1|C2|C3)"
echo "Switch windows: Ctrl-B then a number, or click a window name in the status bar."
echo "Attach with:   tmux attach -t $SESSION_NAME"
echo "Kill it with:  tmux kill-session -t $SESSION_NAME"

tmux attach -t "$SESSION_NAME"
