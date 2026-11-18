#!/usr/bin/env bash
#
# Starts one device's share of the M4 system, with one tmux pane per process.
#
# Usage:
#   ./run_device_M4.sh sacred <active|passive>
#   ./run_device_M4.sh replica <1|2|3> <active|passive> <SACRED_IP>
#
# The fault-tolerance infrastructure starts at once. Replica and client commands
# are typed into their panes but not run, so you launch them in the order the
# rubric asks for: press Enter in each pane when it is that process's turn.
#
# To inject a fault, press Ctrl-C in a replica's pane. To recover it, press
# Up then Enter in the same pane.
#
# Optional settings (use the same values on every device):
#   CHECKPOINT_FREQ       seconds between passive checkpoints (default 2)
#   HEARTBEAT_FREQ        seconds between heartbeats (default 1)
#   LFD_TIMEOUT           seconds before the GFD presumes a machine failed (default 5)
#   REQUEST_INTERVAL      seconds between client requests (default 1)
#   STATE_TRANSFER_DELAY  seconds a replica stays not ready (default 3)
#   GFD_PORT, RM_PORT     default 6000 and 6100
#   REPLICA_BASE_PORT     replica N listens on this plus N (default 5000)

set -euo pipefail

usage() {
    echo "Usage: $0 sacred <active|passive>" >&2
    echo "       $0 replica <1|2|3> <active|passive> <SACRED_IP>" >&2
    exit 1
}

ROLE="${1:-}"

case "$ROLE" in
    sacred)
        [[ $# -eq 2 ]] || usage
        MODE="$2"
        SESSION_NAME="${SESSION_NAME:-m4-sacred}"
        ;;
    replica)
        [[ $# -eq 4 ]] || usage
        REPLICA_NUM="$2"
        MODE="$3"
        SACRED_IP="$4"
        [[ "$REPLICA_NUM" == 1 || "$REPLICA_NUM" == 2 || "$REPLICA_NUM" == 3 ]] || usage
        SESSION_NAME="${SESSION_NAME:-m4-replica$REPLICA_NUM}"
        ;;
    *)
        usage
        ;;
esac

[[ "$MODE" == "active" || "$MODE" == "passive" ]] || usage

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
GFD_PORT="${GFD_PORT:-6000}"
RM_PORT="${RM_PORT:-6100}"
REPLICA_BASE_PORT="${REPLICA_BASE_PORT:-5000}"
CHECKPOINT_FREQ="${CHECKPOINT_FREQ:-2}"
HEARTBEAT_FREQ="${HEARTBEAT_FREQ:-1}"
LFD_TIMEOUT="${LFD_TIMEOUT:-5}"
REQUEST_INTERVAL="${REQUEST_INTERVAL:-1}"
STATE_TRANSFER_DELAY="${STATE_TRANSFER_DELAY:-3}"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is required. Install it with: brew install tmux" >&2
    exit 1
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "Session $SESSION_NAME already exists. Stop it with:" >&2
    echo "    tmux kill-session -t $SESSION_NAME" >&2
    exit 1
fi

if [[ "$ROLE" == "sacred" ]]; then
    ports_to_check=("$RM_PORT" "$GFD_PORT")
else
    ports_to_check=("$((REPLICA_BASE_PORT + REPLICA_NUM))")
fi

for port in "${ports_to_check[@]}"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "Port $port is already in use. Stop the older process first." >&2
        lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2 || true
        exit 1
    fi
done

"$PYTHON_BIN" -m py_compile "$PROJECT_DIR"/src/*.py

tmux new-session -d -s "$SESSION_NAME" -n M4 -c "$PROJECT_DIR" -x 200 -y 60
tmux set-window-option -t "$SESSION_NAME:M4" pane-border-status top
tmux set-window-option -t "$SESSION_NAME:M4" pane-border-format ' #{pane_title} '
tmux set-option -t "$SESSION_NAME" mouse on

last_pane=""

# Create a titled pane and type a command into it, without pressing Enter.
# The new pane's ID is left in last_pane.
type_command() {
    local title="$1"
    shift

    # Splitting the newest pane keeps the panes in launch order on screen.
    if [[ -z "$last_pane" ]]; then
        last_pane="$(tmux display-message -p -t "$SESSION_NAME:M4" '#{pane_id}')"
    else
        last_pane="$(tmux split-window -d -P -F '#{pane_id}' \
            -t "$last_pane" -c "$PROJECT_DIR")"
        tmux select-layout -t "$SESSION_NAME:M4" tiled >/dev/null
    fi

    tmux select-pane -t "$last_pane" -T "$title"
    tmux send-keys -t "$last_pane" -l "$*"
}

run_command() {
    type_command "$@"
    tmux send-keys -t "$last_pane" C-m
}

if [[ "$ROLE" == "sacred" ]]; then
    run_command "RM" "$PYTHON_BIN" -u src/replication_manager.py \
        --host 0.0.0.0 --port "$RM_PORT" --replication-mode "$MODE"
    sleep 1

    run_command "GFD" "$PYTHON_BIN" -u src/global_fault_detector.py \
        --host 0.0.0.0 --port "$GFD_PORT" \
        --rm-host 127.0.0.1 --rm-port "$RM_PORT" \
        --lfd-timeout "$LFD_TIMEOUT"

    for client_num in 1 2 3; do
        type_command "C$client_num" "$PYTHON_BIN" -u src/client.py \
            --client-id "C$client_num" \
            --gfd-host 127.0.0.1 --gfd-port "$GFD_PORT" \
            --replication-mode "$MODE" \
            --add-amount "$client_num" \
            --request-interval "$REQUEST_INTERVAL"
    done

    this_ip="$(cd "$PROJECT_DIR/src" && PYTHONDONTWRITEBYTECODE=1 "$PYTHON_BIN" -c \
        'import net_utils; print(net_utils.local_ip_toward("8.8.8.8"))' 2>/dev/null || echo unknown)"

    cat <<MESSAGE

Sacred machine is up in $MODE mode. Its IP address is: $this_ip

On each replica machine, run:
  ./run_device_M4.sh replica <1|2|3> $MODE $this_ip

Once the replicas are up, press Enter in the C1, C2, and C3 panes, one at a time.
MESSAGE
else
    replica_port="$((REPLICA_BASE_PORT + REPLICA_NUM))"

    run_command "LFD$REPLICA_NUM" "$PYTHON_BIN" -u src/local_fault_detector.py \
        --lfd-id "LFD$REPLICA_NUM" --replica-id "S$REPLICA_NUM" \
        --server-host 127.0.0.1 --server-port "$replica_port" \
        --heartbeat-freq "$HEARTBEAT_FREQ" \
        --gfd-host "$SACRED_IP" --gfd-port "$GFD_PORT"

    replica_arguments=(
        --replica-id "S$REPLICA_NUM"
        --host 0.0.0.0
        --port "$replica_port"
        --replication-mode "$MODE"
        --rm-host "$SACRED_IP"
        --rm-port "$RM_PORT"
        --state-transfer-delay "$STATE_TRANSFER_DELAY"
    )

    if [[ "$MODE" == "passive" ]]; then
        replica_arguments+=(--checkpoint-freq "$CHECKPOINT_FREQ")
    fi

    type_command "S$REPLICA_NUM" "$PYTHON_BIN" -u src/server_replica.py \
        "${replica_arguments[@]}"

    cat <<MESSAGE

LFD$REPLICA_NUM is up and connecting to the GFD at $SACRED_IP:$GFD_PORT.

When it is this replica's turn, press Enter in the S$REPLICA_NUM pane.
In passive mode, the first replica started becomes the primary.
MESSAGE
fi

cat <<MESSAGE

  Fault:     press Ctrl-C in a replica's pane
  Recovery:  in the same pane, press Up then Enter
  Detach:    Ctrl-b then d  (reopen with: tmux attach -t $SESSION_NAME)
  Stop all:  tmux kill-session -t $SESSION_NAME
MESSAGE

tmux attach-session -t "$SESSION_NAME"
