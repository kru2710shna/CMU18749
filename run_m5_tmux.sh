#!/usr/bin/env bash
#
# Run the complete Milestone 5 system locally with one tmux pane per component.
# Recovered replicas open replacement tmux windows, keeping their output out of
# the LFD panes while the RM -> GFD -> LFD recovery path remains unchanged.
#
# Usage:
#   ./run_m5_tmux.sh <active|passive> [initial-primary-number]
#
# Examples:
#   ./run_m5_tmux.sh active
#   ./run_m5_tmux.sh passive 3
#
# Optional environment variables:
#   PYTHON_BIN, SESSION_NAME, HEARTBEAT_FREQ, CHECKPOINT_FREQ,
#   REQUEST_INTERVAL, STATE_TRANSFER_DELAY, RECOVERY_RETRY,
#   RM_PORT, GFD_PORT, REPLICA_BASE_PORT, STARTUP_WAIT

set -euo pipefail

MODE="${1:-}"
PRIMARY_NUM="${2:-1}"

if [[ "$MODE" != "active" && "$MODE" != "passive" ]]; then
    echo "Usage: $0 <active|passive> [initial-primary-number]" >&2
    exit 1
fi
if [[ "$PRIMARY_NUM" != 1 && "$PRIMARY_NUM" != 2 && "$PRIMARY_NUM" != 3 ]]; then
    echo "initial-primary-number must be 1, 2, or 3." >&2
    exit 1
fi
if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is required (for example: brew install tmux)." >&2
    exit 1
fi

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SESSION_NAME="${SESSION_NAME:-m5-local}"
HEARTBEAT_FREQ="${HEARTBEAT_FREQ:-1}"
CHECKPOINT_FREQ="${CHECKPOINT_FREQ:-2}"
REQUEST_INTERVAL="${REQUEST_INTERVAL:-1}"
STATE_TRANSFER_DELAY="${STATE_TRANSFER_DELAY:-3}"
RECOVERY_RETRY="${RECOVERY_RETRY:-2}"
RM_PORT="${RM_PORT:-6100}"
GFD_PORT="${GFD_PORT:-6000}"
REPLICA_BASE_PORT="${REPLICA_BASE_PORT:-5000}"
STARTUP_WAIT="${STARTUP_WAIT:-6}"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "tmux session '$SESSION_NAME' already exists. Stop it with:" >&2
    echo "  tmux kill-session -t $SESSION_NAME" >&2
    exit 1
fi

"$PYTHON_BIN" -m py_compile "$PROJECT_DIR"/src/*.py \
    "$PROJECT_DIR"/launch_component_terminal.py

# Put a safely shell-quoted argv in the global REPLY variable.
shell_join() {
    local argument quoted
    REPLY=""
    for argument in "$@"; do
        printf -v quoted '%q' "$argument"
        REPLY+="${REPLY:+ }$quoted"
    done
}

configure_pane() {
    local pane_id="$1"
    local title="$2"
    tmux select-pane -t "$pane_id" -T "$title"
}

new_window_component() {
    local window_name="$1"
    local title="$2"
    shift 2
    shell_join "$@"
    LAST_PANE="$(tmux new-window -d -P -F '#{pane_id}' \
        -t "$SESSION_NAME:" -n "$window_name" -c "$PROJECT_DIR" "$REPLY")"
    configure_pane "$LAST_PANE" "$title"
}

split_component() {
    local target_pane="$1"
    local title="$2"
    shift 2
    shell_join "$@"
    LAST_PANE="$(tmux split-window -d -P -F '#{pane_id}' \
        -t "$target_pane" -c "$PROJECT_DIR" "$REPLY")"
    configure_pane "$LAST_PANE" "$title"
}

server_command() {
    local replica_num="$1"
    local replica_id="S$replica_num"
    local replica_port="$((REPLICA_BASE_PORT + replica_num))"
    SERVER_COMMAND=(
        "$PYTHON_BIN" -u "$PROJECT_DIR/src/server_replica.py"
        --replica-id "$replica_id"
        --host 127.0.0.1
        --port "$replica_port"
        --replication-mode "$MODE"
        --rm-host 127.0.0.1
        --rm-port "$RM_PORT"
        --state-transfer-delay "$STATE_TRANSFER_DELAY"
    )
    if [[ "$MODE" == "passive" ]]; then
        SERVER_COMMAND+=(--checkpoint-freq "$CHECKPOINT_FREQ")
    fi
}

# Infrastructure window.
shell_join "$PYTHON_BIN" -u "$PROJECT_DIR/src/replication_manager.py" \
    --host 127.0.0.1 --port "$RM_PORT" --replication-mode "$MODE" \
    --auto-recovery --recovery-retry "$RECOVERY_RETRY"
tmux new-session -d -s "$SESSION_NAME" -n infrastructure \
    -c "$PROJECT_DIR" -x 220 -y 65 "$REPLY"
tmux set-option -t "$SESSION_NAME" mouse on
tmux set-window-option -g -t "$SESSION_NAME" pane-border-status top
tmux set-window-option -g -t "$SESSION_NAME" pane-border-format ' #{pane_title} '
infra_pane="$(tmux display-message -p -t "$SESSION_NAME:infrastructure" '#{pane_id}')"
configure_pane "$infra_pane" RM

sleep 0.5
split_component "$infra_pane" GFD \
    "$PYTHON_BIN" -u "$PROJECT_DIR/src/global_fault_detector.py" \
    --host 127.0.0.1 --port "$GFD_PORT" \
    --rm-host 127.0.0.1 --rm-port "$RM_PORT"
tmux select-layout -t "$SESSION_NAME:infrastructure" even-horizontal >/dev/null

# LFD window. Each recovery command uses the terminal helper, which stays alive
# while the replacement server window exists. This preserves LFD idempotence.
lfd_first_pane=""
for replica_num in 1 2 3; do
    replica_id="S$replica_num"
    lfd_id="LFD$replica_num"
    replica_port="$((REPLICA_BASE_PORT + replica_num))"
    server_command "$replica_num"
    recovery_command=(
        "$PYTHON_BIN" -u "$PROJECT_DIR/launch_component_terminal.py"
        --backend tmux
        --tmux-session "$SESSION_NAME"
        --title "$replica_id-recovered"
        "${SERVER_COMMAND[@]}"
    )
    lfd_command=(
        "$PYTHON_BIN" -u "$PROJECT_DIR/src/local_fault_detector.py"
        --lfd-id "$lfd_id"
        --replica-id "$replica_id"
        --server-host 127.0.0.1
        --server-port "$replica_port"
        --heartbeat-freq "$HEARTBEAT_FREQ"
        --gfd-host 127.0.0.1
        --gfd-port "$GFD_PORT"
        --replica-public-host 127.0.0.1
        --recovery-command "${recovery_command[@]}"
    )
    if [[ -z "$lfd_first_pane" ]]; then
        new_window_component detectors "$lfd_id" "${lfd_command[@]}"
        lfd_first_pane="$LAST_PANE"
    else
        split_component "$lfd_first_pane" "$lfd_id" "${lfd_command[@]}"
    fi
done
tmux select-layout -t "$SESSION_NAME:detectors" even-horizontal >/dev/null

# Launch the requested passive primary first and wait before its competitors.
server_command "$PRIMARY_NUM"
new_window_component replicas "S$PRIMARY_NUM" "${SERVER_COMMAND[@]}"
server_first_pane="$LAST_PANE"
sleep 2
for replica_num in 1 2 3; do
    if [[ "$replica_num" == "$PRIMARY_NUM" ]]; then
        continue
    fi
    server_command "$replica_num"
    split_component "$server_first_pane" "S$replica_num" "${SERVER_COMMAND[@]}"
done
tmux select-layout -t "$SESSION_NAME:replicas" even-horizontal >/dev/null

# Allow membership and initial state transfer to settle before clients start.
sleep "$STARTUP_WAIT"
client_first_pane=""
for client_num in 1 2 3; do
    client_command=(
        "$PYTHON_BIN" -u "$PROJECT_DIR/src/client.py"
        --client-id "C$client_num"
        --gfd-host 127.0.0.1
        --gfd-port "$GFD_PORT"
        --replication-mode "$MODE"
        --add-amount "$client_num"
        --request-interval "$REQUEST_INTERVAL"
    )
    if [[ -z "$client_first_pane" ]]; then
        new_window_component clients "C$client_num" "${client_command[@]}"
        client_first_pane="$LAST_PANE"
    else
        split_component "$client_first_pane" "C$client_num" "${client_command[@]}"
    fi
done
tmux select-layout -t "$SESSION_NAME:clients" even-horizontal >/dev/null

cat <<MESSAGE

Milestone 5 local $MODE demo is running in tmux session '$SESSION_NAME'.

Windows:
  infrastructure  RM and GFD
  detectors       LFD1, LFD2, LFD3
  replicas        initial S1, S2, S3
  clients         C1, C2, C3

Fault injection:
  Focus any replica pane and press Ctrl-C. The RM will automatically open a
  replacement window named SN-recovered. Press Ctrl-C there to reinject the
  fault into the recovered process.

Navigation: Ctrl-b n / Ctrl-b p, or use the mouse
Detach:     Ctrl-b d
Reattach:   tmux attach -t $SESSION_NAME
Stop all:   tmux kill-session -t $SESSION_NAME
MESSAGE

tmux select-window -t "$SESSION_NAME:infrastructure"
tmux attach-session -t "$SESSION_NAME"
