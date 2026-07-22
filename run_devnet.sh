#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# allegro local devnet
#   usage: ./run_devnet.sh [NODES=2] [BASE_PORT=13000] [--execution reth|stub]
# ─────────────────────────────────────────────────────────────
set -euo pipefail

N=${1:-2}
BASE_PORT=${2:-13000}
EXECUTION=${3:-reth}  # "reth" or "stub"
DATA_DIR=${TMPDIR:-/tmp}/allegro-devnet-$$

# Reth ports (incremented per node)
RPC_BASE=8545
AUTHRPC_BASE=8551
RETH_P2P_BASE=30303

cleanup() {
    echo ""
    echo "=== stopping nodes ==="
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT INT TERM

# ── 1. build (quietly) ──────────────────────────────────────
echo "=== building ==="
cargo build -p allegro -p allegro-xtask -q 2>&1 | grep -v "^$" || true
BINARY=target/debug/allegro
XTASK=target/debug/allegro-xtask

# ── 2. genesis ──────────────────────────────────────────────
echo "=== generating genesis ($N validators) ==="
mkdir -p "$DATA_DIR"
"$XTASK" genesis --validators "$N" --base-port "$BASE_PORT" --output "$DATA_DIR"
echo ""

# ── 3. start nodes ──────────────────────────────────────────
pids=()
for i in $(seq 0 $((N - 1))); do
    port=$((BASE_PORT + i))
    # build --peer flags for all OTHER nodes
    peers=""
    for j in $(seq 0 $((N - 1))); do
        [ "$j" -ne "$i" ] && peers="$peers --peer 127.0.0.1:$((BASE_PORT + j))"
    done
    log="$DATA_DIR/node-$i.log"

    # reth-specific args
    reth_args=""
    if [ "$EXECUTION" = "reth" ]; then
        reth_args=" \
            --datadir $DATA_DIR/node-$i \
            --rpc-port $((RPC_BASE + i)) \
            --authrpc-port $((AUTHRPC_BASE + i)) \
            --reth-p2p-port $((RETH_P2P_BASE + i))"
    fi

    # start node with line-buffered stderr
    RUST_LOG="allegro=info,commonware=warn" "$BINARY" \
        --execution "$EXECUTION" \
        --node "$i" \
        --listen "127.0.0.1:$port" \
        --leader-timeout 1000 \
        --cert-timeout 2000 \
        $peers \
        $reth_args > /dev/null 2>"$log" &
    pids+=($!)
    echo "  node $i : pid $! : p2p port $port"
    if [ -n "$reth_args" ]; then
        echo "           : reth rpc  http://127.0.0.1:$((RPC_BASE + i))"
    fi
done

# ── 4. wait for startup ─────────────────────────────────────
echo ""
echo "=== waiting for all nodes to start ==="
for i in $(seq 1 30); do
    sleep 1
    alive=0
    for pid in "${pids[@]}"; do
        kill -0 "$pid" 2>/dev/null && alive=$((alive + 1))
    done
    [ "$alive" -eq "$N" ] && break
done
if ! kill -0 "${pids[0]}" 2>/dev/null; then
    echo "ERROR: nodes failed to start. Check logs in $DATA_DIR"
    cat "$DATA_DIR/node-0.log"
    exit 1
fi

# ── 5. check block production ───────────────────────────────
echo "=== checking block production (initial wait 15s) ==="
sleep 15

# Check via RPC
if [ "$EXECUTION" = "reth" ]; then
    total=0
    for i in $(seq 0 $((N - 1))); do
        rpc_url="http://127.0.0.1:$((RPC_BASE + i))"
        block_num_hex=$(curl -s -X POST "$rpc_url" \
            --header 'Content-Type: application/json' \
            --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
            | grep -o '"result":"0x[^"]*"' | cut -d'"' -f4 2>/dev/null || echo "0x0")
        block_num_dec=$((block_num_hex))
        total=$((total + block_num_dec))
        echo "  node $i : eth_blockNumber = $block_num_hex ($block_num_dec blocks)"
    done
    
    if [ "$total" -lt $((N * 2)) ]; then
        echo ""
        echo "WARNING: only $total total blocks across $N nodes (expected at least $((N * 2)))"
        for i in $(seq 0 $((N - 1))); do
            echo "Last 5 lines from node-$i.log:"
            tail -5 "$DATA_DIR/node-$i.log"
        done
    fi
else
    # stub mode: check via logs
    for i in $(seq 0 $((N - 1))); do
        log="$DATA_DIR/node-$i.log"
        count=$(grep -c -e "handle_propose" -e "proposed block" "$log" 2>/dev/null || true)
        echo "  node $i : $count proposals (log)"
    done
fi

echo ""
echo "============================================"
echo " Devnet running: $N nodes"
echo " Logs: $DATA_DIR"
echo "============================================"

# ── 6. optional: send a test tx (reth mode, needs cast) ─────
if [ "$EXECUTION" = "reth" ] && command -v cast &>/dev/null; then
    echo ""
    echo "=== sending test tx via cast ==="
    RPC_URL="http://127.0.0.1:$RPC_BASE"
    # Anvil's default test account 0 (from reth DEV spec):
    #   addr: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
    #   key:  0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
    cast send --rpc-url "$RPC_URL" \
        --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
        0x000000000000000000000000000000000000dEaD \
        --value 0.01ether \
        --legacy \
        2>/dev/null \
        && echo "  tx submitted successfully" \
        || echo "  tx submission skipped (cast available but may have failed)"
fi

# keep running until Ctrl+C
echo ""
echo "Press Ctrl+C to stop."
wait
