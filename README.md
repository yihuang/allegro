# Allegro

**Allegro** is a minimal integration of [Reth](https://github.com/paradigmxyz/reth) (execution layer) with [Commonware](https://github.com/commonwarexyz/monorepo) consensus — inspired by [Tempo](https://github.com/tempoxyz/tempo) and Commonware's [Alto](https://github.com/commonwarexyz/monorepo/tree/main/consensus) demo.

## Architecture

```
crates/primitives/     -- AllegroHeader, AllegroConsensusContext, Digest, Block aliases
crates/consensus/      -- Commonware simplex engine (Automaton, application actor, codec)
crates/reth/           -- Lightweight reth helper (create_reth_payload_builder, attrs helpers)
crates/node/           -- Real reth integration (chainspec, launch, builder closures, finalizer)
bin/allegro/           -- CLI entry point (stub or reth execution mode)
docs/                  -- Integration analysis and implementation plan
```

### Execution Modes

- **`--execution reth`** (default) — embeds a full `EthereumNode` (database, EVM, tx pool, JSON-RPC). Blocks are built via the engine API (`fork_choice_updated` + `get_payload`). The consensus engine runs in a separate OS thread with its own tokio runtime.
- **`--execution stub`** — standalone consensus node using empty-block stub builder. No database, no EVM. Used for testing and light development.

### Component Status

| Component | Status | Details |
|-----------|--------|---------|
| Consensus (simplex engine) | ✅ | Commonware threshold simplex with block relay and receiver |
| Stub payload builder | ✅ | Empty blocks with AllegroHeader consensus context |
| Real payload builder | ✅ | Reth engine API backed, with self-import and ForkchoiceTracker |
| Finalization forwarder | ✅ | Routes finalization certificates to reth via FCU |
| Block-info tracking | ✅ | BlockInfoMap tracks number/hash/timestamp across propose & verify |
| Genesis hash config | ✅ | Configurable via EngineConfig.genesis_hash (ZERO for stub) |
| Timestamp enforcement | ✅ | `timestamp = max(now, parent_ts + 1)` to satisfy reth FCU validation |
| Cancun+ attrs | ✅ | `parent_beacon_block_root: Some(ZERO)`, `withdrawals: Some([])` |
| JSON-RPC | ✅ | Default on port 8545 (per node, indexed) |
| Devnet script | ✅ | `run_devnet.sh` supports both reth and stub modes |
| Engine roundtrip test | 🟡 | Test skeleton written but hangs on node launch (port / Runtime conflict) |

## Quickstart

### Prerequisites

- Rust 1.85+ (edition 2021)
- `cast` (foundry) for on-chain interaction (optional)

### Build

```bash
cargo build --workspace
```

### Run a devnet (reth mode, default)

```bash
./run_devnet.sh 2
```

This starts 2 nodes with:
- Reth JSON-RPC on `http://127.0.0.1:8545` (node 0) and `:8546` (node 1)
- Engine API auth on `:8551` / `:8552`
- Reth P2P on `:30303` / `:30304`
- Consensus P2P on `:13000` / `:13001`

Wait ~10 seconds for the first blocks, then query:

```bash
cast block-number --rpc-url http://127.0.0.1:8545
# → 1 (or higher)
```

### Run a devnet (stub mode)

```bash
./run_devnet.sh 2 13000 --execution stub
```

### Send a test transaction (reth mode)

```bash
cast send --rpc-url http://127.0.0.1:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  0x000000000000000000000000000000000000dEaD \
  --value 0.01ether --legacy
```

### Run tests

```bash
cargo test -p allegro-primitives -p allegro-consensus -p allegro-reth -p allegro-node
```

## Project Structure

```
bin/allegro/           -- Binary entry point (stub and reth modes)
crates/
  primitives/          -- Digest, AllegroHeader, type aliases
  consensus/           -- Commonware simplex engine + payload builder trait
  reth/                -- Closure-based payload builder constructor, attrs helpers
  node/                -- Reth integration: chainspec, launch, builder closures, finalizer
xtask/                 -- Devnet genesis key generation
docs/
  reth-integration-analysis.md   -- Architecture analysis (Tempo comparison)
  reth-integration-plan.md       -- Detailed implementation plan
```

## License

MIT
