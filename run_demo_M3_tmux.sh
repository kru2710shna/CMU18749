#!/usr/bin/env bash
#
# One-window M3 demo launcher.
#
# Run this from the CMU18749-1 project root in a large/maximized Terminal
# window. It creates ONE tmux window containing ten panes in this layout:
#
#   GFD | S1 PRIMARY | S2 BACKUP | S3 BACKUP | LFD1 -> S1
#  LFD2 -> S2 | LFD3 -> S3 | C1 | C2 | C3
#
# Click a pane and press Ctrl-C to fault/stop only that process. The pane stays
# visible after the process exits so its final log lines remain on screen.

set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SESSION_NAME="${SESSION_NAME:-m3-demo}"
HOST="${HOST:-127.0.0.1}"
GFD_PORT="${GFD_PORT:-6000}"
CHECKPOINT_FREQ="${CHECKPOINT_FREQ:-2}"
REQUEST_INTERVAL="${REQUEST_INTERVAL:-1}"
REPLY_TIMEOUT_MS="${REPLY_TIMEOUT_MS:-2000}"

COLS=5
ROWS=2

required_files=(
    src/client.py
    src/global_fault_detector.py
    src/local_fault_detector.py
    src/log_utils.py
    src/net_utils.py
    src/protocol.py
    src/server_replica.py
)

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "Cannot find Python executable: $PYTHON_BIN" >&2
    exit 1
fi

if ! command -v tmux >/dev/null 2>&1; then
    cat >&2 <<'MESSAGE'
tmux is required for the one-window, ten-pane dashboard.

Install it once with:
    brew install tmux

Then run this script again.
MESSAGE
    exit 1
fi

for file in "${required_files[@]}"; do
    if [[ ! -f "$PROJECT_DIR/$file" ]]; then
        echo "Missing required file: $PROJECT_DIR/$file" >&2
        exit 1
    fi
done

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    cat >&2 <<MESSAGE
The tmux session '$SESSION_NAME' already exists.

Resume it with:
    tmux attach -t $SESSION_NAME

Or, after you have finished the old demo, remove it with:
    tmux kill-session -t $SESSION_NAME
MESSAGE
    exit 1
fi

# Avoid a confusing half-started demo when an earlier manual run still owns a
# required port. We deliberately do not kill anything automatically.
for port in "$GFD_PORT" 5001 5002 5003 5102 5103; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "Port $port is already in use. Stop the older M3 process first." >&2
        lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2 || true
        exit 1
    fi
done

echo "Checking Python syntax..."
"$PYTHON_BIN" -m py_compile "$PROJECT_DIR"/src/*.py

terminal_cols="$(tput cols 2>/dev/null || printf '120')"
terminal_lines="$(tput lines 2>/dev/null || printf '36')"

if (( terminal_cols < 120 || terminal_lines < 30 )); then
    cat <<MESSAGE

Warning: the current Terminal is ${terminal_cols} columns x ${terminal_lines} rows.
For readable logs in five columns, maximize the Terminal window and/or use a
smaller Terminal font. tmux will resize the panes automatically if you enlarge
the window after it starts.
MESSAGE
fi

# Give each newly-created right/bottom pane an approximately equal size. tmux
# maintains the same proportional grid when the enclosing Terminal is resized.
column_width=$((terminal_cols / COLS))
row_height=$((terminal_lines / ROWS))

build_command() {
    local title="$1"
    shift

    local quoted_project quoted_args quoted_header
    printf -v quoted_project '%q' "$PROJECT_DIR"
    printf -v quoted_args '%q ' "$@"
    printf -v quoted_header '%q' "=== $title ==="

    printf 'cd %s && clear && printf "%%s\\n" %s && exec %s' \
        "$quoted_project" "$quoted_header" "$quoted_args"
}

send_process() {
    local pane_id="$1"
    local title="$2"
    shift 2

    local command
    command="$(build_command "$title" "$@")"

    tmux select-pane -t "$pane_id" -T "$title"
    tmux send-keys -t "$pane_id" -l "$command"
    tmux send-keys -t "$pane_id" C-m
}

pane_ids_in_screen_order() {
    # Sort panes top-to-bottom, then left-to-right. The third field is the
    # pane ID; this is stable even though tmux's numeric pane indexes change
    # while the layout is being built.
    tmux list-panes -t "$SESSION_NAME:0" \
        -F '#{pane_top} #{pane_left} #{pane_id}' \
        | sort -n -k1,1 -k2,2 \
        | awk '{print $3}'
}

echo "Creating one tmux window with a ${COLS} x ${ROWS} M3 grid..."
tmux new-session -d -s "$SESSION_NAME" -n M3 \
    -x "$terminal_cols" -y "$terminal_lines"

initial_pane="$(tmux display-message -p -t "$SESSION_NAME:0.0" '#{pane_id}')"

# Create five columns. Always split the remaining large LEFT pane, so each
# new pane gets one column-width rather than repeatedly splitting a small pane.
for _ in 1 2 3 4; do
    tmux split-window -d -h -t "$initial_pane" -l "$column_width" >/dev/null
done

# Split each top-row column into a top and bottom pane.
top_row_panes=()
while IFS= read -r pane_id; do
    top_row_panes+=("$pane_id")
done < <(pane_ids_in_screen_order)

for pane_id in "${top_row_panes[@]}"; do
    tmux split-window -d -v -t "$pane_id" -l "$row_height" >/dev/null
done

pane_ids=()
while IFS= read -r pane_id; do
    pane_ids+=("$pane_id")
done < <(pane_ids_in_screen_order)

if (( ${#pane_ids[@]} != 10 )); then
    echo "Expected 10 panes, created ${#pane_ids[@]}." >&2
    tmux kill-session -t "$SESSION_NAME"
    exit 1
fi

# Keep exited backup panes on screen during the fault demo and label all panes.
tmux set-window-option -t "$SESSION_NAME:0" remain-on-exit on
tmux set-window-option -t "$SESSION_NAME:0" pane-border-status top
tmux set-window-option -t "$SESSION_NAME:0" pane-border-format ' #{pane_title} '
tmux set-option -t "$SESSION_NAME" mouse on

# Top row: GFD | S1 | S2 | S3 | LFD1
# Bottom:  LFD2 | LFD3 | C1 | C2 | C3
gfd_pane="${pane_ids[0]}"
s1_pane="${pane_ids[1]}"
s2_pane="${pane_ids[2]}"
s3_pane="${pane_ids[3]}"
lfd1_pane="${pane_ids[4]}"
lfd2_pane="${pane_ids[5]}"
lfd3_pane="${pane_ids[6]}"
c1_pane="${pane_ids[7]}"
c2_pane="${pane_ids[8]}"
c3_pane="${pane_ids[9]}"

send_process "$gfd_pane" "GFD" \
    "$PYTHON_BIN" -u src/global_fault_detector.py \
    --host "$HOST" --port "$GFD_PORT"
sleep 1

# Backups are launched before S1 so S1's first periodic checkpoint can connect.
send_process "$s2_pane" "S2 BACKUP" \
    "$PYTHON_BIN" -u src/server_replica.py \
    --replica-id S2 --host "$HOST" --port 5002 \
    --replication-mode passive --role backup \
    --checkpoint-host "$HOST" --checkpoint-port 5102

send_process "$s3_pane" "S3 BACKUP" \
    "$PYTHON_BIN" -u src/server_replica.py \
    --replica-id S3 --host "$HOST" --port 5003 \
    --replication-mode passive --role backup \
    --checkpoint-host "$HOST" --checkpoint-port 5103
sleep 1

send_process "$s1_pane" "S1 PRIMARY" \
    "$PYTHON_BIN" -u src/server_replica.py \
    --replica-id S1 --host "$HOST" --port 5001 \
    --replication-mode passive --role primary \
    --checkpoint-freq "$CHECKPOINT_FREQ" \
    --backups "S2:${HOST}:5102,S3:${HOST}:5103"
sleep 1

send_process "$lfd1_pane" "LFD1 -> S1" \
    "$PYTHON_BIN" -u src/local_fault_detector.py \
    --lfd-id LFD1 --replica-id S1 \
    --server-host "$HOST" --server-port 5001 \
    --heartbeat-freq 1 \
    --gfd-host "$HOST" --gfd-port "$GFD_PORT" \
    --replica-public-host "$HOST"

send_process "$lfd2_pane" "LFD2 -> S2" \
    "$PYTHON_BIN" -u src/local_fault_detector.py \
    --lfd-id LFD2 --replica-id S2 \
    --server-host "$HOST" --server-port 5002 \
    --heartbeat-freq 1 \
    --gfd-host "$HOST" --gfd-port "$GFD_PORT" \
    --replica-public-host "$HOST"

send_process "$lfd3_pane" "LFD3 -> S3" \
    "$PYTHON_BIN" -u src/local_fault_detector.py \
    --lfd-id LFD3 --replica-id S3 \
    --server-host "$HOST" --server-port 5003 \
    --heartbeat-freq 1 \
    --gfd-host "$HOST" --gfd-port "$GFD_PORT" \
    --replica-public-host "$HOST"

# The clients also tolerate an initially empty membership, but this makes the
# first screen more readable because the GFD normally has all three members.
sleep 2

send_process "$c1_pane" "C1" \
    "$PYTHON_BIN" -u src/client.py \
    --client-id C1 --gfd-host "$HOST" --gfd-port "$GFD_PORT" \
    --replication-mode passive --primary-id S1 \
    --add-amount 1 --request-interval "$REQUEST_INTERVAL" \
    --reply-timeout-ms "$REPLY_TIMEOUT_MS"

send_process "$c2_pane" "C2" \
    "$PYTHON_BIN" -u src/client.py \
    --client-id C2 --gfd-host "$HOST" --gfd-port "$GFD_PORT" \
    --replication-mode passive --primary-id S1 \
    --add-amount 2 --request-interval "$REQUEST_INTERVAL" \
    --reply-timeout-ms "$REPLY_TIMEOUT_MS"

send_process "$c3_pane" "C3" \
    "$PYTHON_BIN" -u src/client.py \
    --client-id C3 --gfd-host "$HOST" --gfd-port "$GFD_PORT" \
    --replication-mode passive --primary-id S1 \
    --add-amount 3 --request-interval "$REQUEST_INTERVAL" \
    --reply-timeout-ms "$REPLY_TIMEOUT_MS"

tmux select-pane -t "$gfd_pane"

cat <<'MESSAGE'

M3 dashboard is ready in one tmux window.

Click the S2 BACKUP pane and press Ctrl-C for fault 1.
Wait for the GFD pane to show: GFD: 2 members: S1, S3.
Then click the S3 BACKUP pane and press Ctrl-C for fault 2.

Useful tmux controls:
  Ctrl-b d   detach and leave the dashboard running
  tmux attach -t m3-demo   reopen it later
  tmux kill-session -t m3-demo   stop/remove the dashboard after the demo
MESSAGE

tmux attach-session -t "$SESSION_NAME"
