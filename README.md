# Allegro

**Allegro** is a minimal integration of [Reth](https://github.com/paradigmxyz/reth) (execution layer) with [Commonware](https://github.com/commonwarexyz/monorepo) consensus — inspired by [Tempo](https://github.com/tempoxyz/tempo) and Commonware's [Alto](https://github.com/commonwarexyz/monorepo/tree/main/consensus) demo.

## Architecture

```
crates/primitives/    -- AllegroHeader, AllegroConsensusContext, Digest, Block aliases
crates/consensus/     -- Commonware consensus integration (Automaton, executor, block codec)
crates/node/          -- Reth node types (WIP — see below)
bin/allegro/          -- CLI entry point
```

### Key Design Decisions

- **No forks required** — Both Reth and Commonware are consumed as upstream dependencies (pinned git commits). All customization lives in Allegro's own crates.
- **Minimal header extension** — `AllegroHeader` wraps an Ethereum `Header` and adds a trailing `AllegroConsensusContext` (epoch, view, parent_view, proposer Ed25519 key).
- **Actor pattern** — Commonware's `Automaton` trait is implemented on a `Mailbox` type that forwards messages to an actor, following Tempo's architecture.

### Status

| Component | Status |
|---|---|
| `allegro-primitives` | ✅ Complete — header, digest, RLP roundtrip tests |
| `allegro-consensus` | 🏗️ Skeleton — Block codec, validator set, executor stub |
| `allegro-node` | ⏳ Placeholder — requires reth API alignment |
| Binary | 🏗️ Basic CLI with key generation |

### Getting Started

```bash
# Build the workspace
cargo build --workspace

# Run the CLI
cargo run -p allegro -- --help

# Run tests
cargo test -p allegro-primitives
```

### License

MIT OR Apache-2.0
