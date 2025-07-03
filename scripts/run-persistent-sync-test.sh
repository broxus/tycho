#!/usr/bin/env bash
set -euo pipefail

#######################################
# Helpers
#######################################
log() { echo -e "[$(date '+%H:%M:%S')] $*"; }

# Exit with message
_die() { log "❌ $*"; exit 1; }

# Wait for an HTTP endpoint to respond with 2xx/3xx
#   $1 – URL, $2 – retries, $3 – sleep between retries
wait_http() {
  local url=$1 retries=$2 delay=$3
  for ((i=1;i<=retries;i++)); do
    if curl -sSf "$url" >/dev/null; then return 0; fi
    sleep "$delay"
  done
  return 1
}

# Extract a single Prometheus metric value (first matching line)
#   $1 – metrics URL, $2 – metric name
metric() {
    curl -s "$1" | awk -v key="$2" '$1==key {print $2; exit}' || true
}

# Wait until a node is actively collating blocks
#   $1 – metrics URL, $2 – max age (sec), $3 – retries, $4 – delay
wait_collation() {
  local url=$1 max_age=$2 retries=$3 delay=$4
  for ((i=1;i<=retries;i++)); do
    local utime now age
    utime=$(metric "$url" tycho_core_last_mc_block_utime || true)
    if [[ -n "$utime" ]]; then
      now=$(date +%s)
      age=$((now-utime))
      (( age<=max_age )) && return 0
    fi
    sleep "$delay"
  done
  return 1
}

# Ensure persistent state saved
wait_ps_saved() {
  local url=$1 retries=$2 delay=2
  for ((i=1;i<=retries;i++)); do
    [[ "$(metric "$url" tycho_core_ps_subscriber_saved_persistent_states_count)" == "1" ]] && return 0
    sleep "$delay"
  done
  return 1
}

#######################################
# Cleanup on exit / ^C
#######################################
cleanup() {
  log "🧹 Cleaning up tycho_node processes via control sockets…"
  shopt -s nullglob
  for sock in "$TMP_DIR"/.temp/control*.sock; do
    [[ -S "$sock" ]] || continue
    local pid
    pid=$(lsof -nU | awk -v s="$(basename "$sock")" '$0~s{print $2;exit}')
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true && sleep 1 && kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$sock" || true
  done
  pkill -f tycho_node 2>/dev/null || true
  log "✅ Cleanup complete."
}
trap cleanup EXIT INT TERM

#######################################
# Configuration
#######################################
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../" && pwd -P)
TMP_DIR="$REPO_ROOT/target/tmp/network-tests"
NODES_TOTAL=4                # Total nodes described in network config
ACTIVE_NODES=(1 2 3)         # Nodes launched initially
MAX_TRIES=100                # Attempts for RPC call tries
BLOCK_AGE_LIMIT=10           # Seconds
DIFF_TAIL_THRESHOLD=50       # Trigger threshold for node‑4 launch
KEYS_PATH="${TMP_DIR}/keys.json"

cd $REPO_ROOT

#######################################
# Stage repo copy
#######################################

log "🗑  Re‑creating $TMP_DIR"
rm -rf "$TMP_DIR" && mkdir -p "$TMP_DIR"

log "📦 Copying repo snapshot → $TMP_DIR (excluding target/ & zerostate.json)"
rsync -a --exclude 'target' "$REPO_ROOT"/ "$TMP_DIR"
ln -s "$REPO_ROOT/target" "$TMP_DIR/target"
cd "$TMP_DIR"

#######################################
# Config generation
#######################################
log "⚙️  Generating node configs"
just init_zerostate_config --force
just init_node_config --force

jq '.starter.custom_boot_offset="0s"' \
   ./config.json > ./config.json.tmp && mv ./config.json.tmp ./config.json

just gen_network "$NODES_TOTAL" --force

#######################################
# Node management helpers
#######################################
start_node() {
  local id=$1
  log "🚀 Starting node $id"
  just node "$id" >"node${id}.log" 2>&1 &
}

get_rpc()     { jq -r '.rpc.listen_addr'     ".temp/config$1.json"; }
get_metrics() { jq -r '.metrics.listen_addr' ".temp/config$1.json"; }

#######################################
# Launch initial nodes 1‑3
#######################################
for id in "${ACTIVE_NODES[@]}"; do start_node "$id"; done

# Wait for RPC & collation
for id in "${ACTIVE_NODES[@]}"; do
  RPC=$(get_rpc "$id")
  METRICS=$(get_metrics "$id")

  log "⏳ Waiting RPC for node $id ($RPC)"
  wait_http "http://$RPC/" "$MAX_TRIES" 1 \
    || _die "RPC for node $id not ready in time"

  log "⏳ Waiting collation for node $id"
  wait_collation "http://$METRICS/metrics" "$BLOCK_AGE_LIMIT" "$MAX_TRIES" 1 \
    || _die "Node $id not collating blocks"

  log "✅ Node $id is alive & collating"
done

#######################################
# Run CI tests in Docker
#######################################

RPC1=$(get_rpc 1);

docker_bin="docker"
[ -n "${CI:-}" ] && docker_bin="podman"
TEST_IMAGE="ghcr.io/broxus/tycho-tests/tycho-tests-one-to-many-internal-messages:latest"

log "🐳 Pulling test image $TEST_IMAGE"
$docker_bin pull "$TEST_IMAGE"

log "🏃 Running tests inside container"

echo "http://${RPC1}/";
test_env=(
  -e CI_JRPC_ENDPOINT="http://${RPC1}/"
  -e CI_GIVER_ADDRESS=-1:1111111111111111111111111111111111111111111111111111111111111111
  -e CI_GIVER_KEY=$(jq -r '.secret' "$KEYS_PATH")
  -e DEPLOY_VALUE=3000
  -e NETWORK=ci
)

$docker_bin run -d --network host "${test_env[@]}" "$TEST_IMAGE" || _die "Test container failed"

#######################################
# Monitor diff‑tail & trigger node‑4
#######################################
METRICS_NODE3=$(get_metrics 3)
NODE4_STARTED=0

log "Run 4 node"
while :; do
  DIFF_TAIL=$(metric "http://$METRICS_NODE3/metrics" 'tycho_do_collate_block_diff_tail_len{workchain="0"}')
  log "diff‑tail now: $DIFF_TAIL"

  if [[ -n "$DIFF_TAIL" && ${DIFF_TAIL%.*} -gt $DIFF_TAIL_THRESHOLD && $NODE4_STARTED -eq 0 ]]; then
    log "📈 Threshold reached — releasing key‑block & preparing node‑4"

    # Release key‑block on nodes 1‑3. Change some in zerostate
    for id in "${ACTIVE_NODES[@]}"; do
      RPC="$(get_rpc "$id")"
      KEY=$(jq -r '.secret' ./keys.json)
      "$TYCHO_BIN_PATH" tool bc set-param \
        --rpc "http://$RPC/" --key "$KEY" 31 '[
          "0000000000000000000000000000000000000000000000000000000000000000",
          "3333333333333333333333333333333333333333333333333333333333333333",
          "4444444444444444444444444444444444444444444444444444444444444444"
        ]'
    done

    # Ensure persistent state saved
    log "💾 Waiting persistent state save"
    for id in "${ACTIVE_NODES[@]}"; do
      wait_ps_saved "http://$(get_metrics "$id")/metrics" 30 \
        || _die "Node $id failed to save persistent state"
    done
    log "✅ Persistent state saved on 1‑3"

    # Start node‑4
    start_node 4; NODE4_STARTED=1

    RPC4=$(get_rpc 4); METRICS4=$(get_metrics 4)

    wait_http "http://$RPC4/" "$MAX_TRIES" 1         || _die "RPC node‑4 timeout"
    wait_collation "http://$METRICS4/metrics" "$BLOCK_AGE_LIMIT" "$MAX_TRIES" 2 || _die "Node‑4 not collating"

    # Final check loop for node‑4
    while true; do
      all_ok=true
      for id in 1 2 3 4; do
        METRICS=$(get_metrics "$id")
        DIFF=$(metric "http://$METRICS/metrics" 'tycho_do_collate_block_diff_tail_len{workchain="0"}')
        WC0=$(metric "http://$METRICS/metrics" 'tycho_collator_block_mismatch_count{workchain="0"}')
        WM1=$(metric "http://$METRICS/metrics" 'tycho_collator_block_mismatch_count{workchain="-1"}')

        log "⏳ node $id diff‑tail=${DIFF:-<none>}";
        if [[ ${WC0:-0} != 0 || ${WM1:-0} != 0 ]]; then _die "Mismatch on node $id (wc0=${WC0:-N/A} wc‑1=${WM1:-N/A})"; fi
        if [[ -z $DIFF || ${DIFF%.*} -gt 1 ]]; then all_ok=false; fi
      done
      $all_ok && { log "🎉 All nodes OK — Test Passed"; exit 0; }
      sleep 2
    done

  fi
  sleep 2
done