# Allegro

[![CI](https://github.com/yihuang/allegro/actions/workflows/ci.yml/badge.svg)](https://github.com/yihuang/allegro/actions/workflows/ci.yml)

**Allegro** embeds a [Reth](https://github.com/paradigmxyz/reth) execution node
inside a [Commonware](https://github.com/commonwarexyz/monorepo) simplex BFT
consensus engine.  One binary, two runtimes — blocks flow through the Engine
API channel handles rather than through JSON-RPC over localhost.

Design and architecture are documented in **[docs/design.md](docs/design.md)**.

## Quickstart

### Build

```bash
cargo build --workspace
```

### Run a 2-node devnet

```bash
./run_devnet.sh 2 13000 reth
```

- Node 0 RPC: `http://127.0.0.1:8545`
- Node 1 RPC: `http://127.0.0.1:8546`

```bash
# Check block production
cast block-number --rpc-url http://127.0.0.1:8545
# → 0x6a9 (both nodes synchronized)

# Send a transaction (uses Anvil account #0)
cast send --rpc-url http://127.0.0.1:8545 \
  0x000000000000000000000000000000000000dEaD \
  --value 0.01ether --legacy
```

### Generate genesis config (optional)

```bash
cargo run -p allegro-xtask -- genesis \
    --validators 4 --base-port 13000 --output ./devnet
```

Produces `genesis.json`, `validators.json`, and per-node key files.

### Cli reference

```
allegro node [reth flags] [--consensus.* flags]   # reth's CLI + consensus extension
allegro stub [--consensus.* flags]                # standalone stub mode (no reth)

# reth flags (full reth node CLI; the usual devnet subset):
#   --chain <PATH|dev>  --datadir <PATH>  --http --http.port <P>
#   --authrpc.port <P>  --port <P>  --disable-discovery  --ipcdisable
# consensus flags:
#   --consensus.node-index <N>          # validator index (0-based)
#   --consensus.listen-address <ADDR>   # consensus p2p address
#   --consensus.peer <ADDR>             # repeat for every peer (all nodes must list each other)
#   --consensus.leader-timeout <MS>     # default: 2000
```

## Architecture

```
crates/primitives/      AllegroHeader, Digest, type aliases
crates/consensus/       Simplex engine: Automaton actor, PayloadBuilder trait,
                        block relay, receiver, finalization sink
                        + Engine API helpers (attrs, closure builder)
crates/node/            Real reth integration: launch, payload builder closures,
                        finalizer, DEV chainspec
bin/allegro/            CLI entry point, dual-runtime orchestration
xtask/                  Genesis generator (Anvil mnemonic, all forks at genesis)
docs/
  design.md             Comprehensive design document
```

## Tests

```bash
# All tests (unit + integration + engine roundtrip + process e2e)
cargo test --workspace

# Consensus-only (fast, no reth compilation)
cargo test -p allegro-primitives -p allegro-consensus

# Key integration: build → validate → finalize → next block (in-process reth)
cargo test -p allegro-node --test engine_roundtrip
```

## Status

| Component | Status |
|-----------|--------|
| Simplex consensus engine | ✅ |
| Stub payload builder (empty blocks) | ✅ |
| Real payload builder (reth Engine API) | ✅ |
| Build → validate → finalize roundtrip | ✅ |
| Dual-tokio-runtime orchestration | ✅ |
| JSON-RPC (`eth_*`, `net_*`, `web3_*`) | ✅ |
| Finalization → forkchoice forwarder | ✅ |
| Multi-node devnet (process e2e) | ✅ |
| Cancun+ payload attributes | ✅ |
| Timestamp enforcement (`max(now, parent_ts + 1)`) | ✅ |
| Genesis generator (`xtask`) | ✅ |
| Devnet script (`run_devnet.sh`) | ✅ |
| Deterministic-runtime tests (SimNetwork) | ✅ |

## License

MIT
