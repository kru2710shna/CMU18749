#!/usr/bin/env bash
#
# Launches the full Milestone 1 demo (S1, LFD1, C1, C2, C3) in one tmux window
# split into 5 panes, so the whole system is visible on one screen instead of
# juggling 5 separate terminal windows. Each pane still runs a real, independent
# process (Ctrl-C on the S1 pane kills only S1, exactly like the rubric expects).
#
# Usage:
#   ./run_demo.sh [heartbeat_freq_seconds]
#
# Example:
#   ./run_demo.sh 1      # LFD1 heartbeats S1 every 1 second
#   ./run_demo.sh 0.5    # restart with a different heartbeat_freq

set -euo pipefail

HEARTBEAT_FREQ="${1:-1}"
SESSION_NAME="m1-demo"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/src" && pwd)"
PORT=5000

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install it with 'brew install tmux' and re-run this script."
    exit 1
fi

# Kill any leftover session from a previous demo run so panes don't stack up.
tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true

tmux new-session -d -s "$SESSION_NAME" -n demo -c "$SRC_DIR"

# Layout: server+LFD stacked on the left, three clients stacked on the right.
tmux split-window -h -t "$SESSION_NAME:demo" -c "$SRC_DIR"
tmux split-window -v -t "$SESSION_NAME:demo.0" -c "$SRC_DIR"
tmux split-window -v -t "$SESSION_NAME:demo.2" -c "$SRC_DIR"
tmux split-window -v -t "$SESSION_NAME:demo.2" -c "$SRC_DIR"

# Pane indices after the splits above:
#   0 = S1 (top-left)      1 = C1
#   2 = LFD1 (bottom-left) 3 = C2
#                          4 = C3
tmux send-keys -t "$SESSION_NAME:demo.0" \
    "python3 server_replica.py --replica-id S1 --host 0.0.0.0 --port $PORT" C-m
sleep 0.5

tmux send-keys -t "$SESSION_NAME:demo.2" \
    "python3 local_fault_detector.py --lfd-id LFD1 --replica-id S1 --server-host localhost --server-port $PORT --heartbeat-freq $HEARTBEAT_FREQ" C-m

tmux send-keys -t "$SESSION_NAME:demo.1" \
    "python3 client.py --client-id C1 --server-host localhost --server-port $PORT" C-m
tmux send-keys -t "$SESSION_NAME:demo.3" \
    "python3 client.py --client-id C2 --server-host localhost --server-port $PORT" C-m
tmux send-keys -t "$SESSION_NAME:demo.4" \
    "python3 client.py --client-id C3 --server-host localhost --server-port $PORT" C-m

tmux select-layout -t "$SESSION_NAME:demo" tiled

echo "Demo running in tmux session '$SESSION_NAME' with heartbeat_freq=$HEARTBEAT_FREQ."
echo "Attach with:   tmux attach -t $SESSION_NAME"
echo "Kill it with:  tmux kill-session -t $SESSION_NAME"

tmux attach -t "$SESSION_NAME"
