# Reth Integration Analysis: Tempo Architecture

## Overview

Tempo embeds reth as its execution layer. The `tempo` binary **is** a reth node — it uses `reth_node_builder::NodeBuilder` to launch reth's database, EVM, transaction pool, networking, and RPC, and then runs the commonware consensus engine alongside.

```
┌─────────────────────────────────────────────────────────────┐
│                     tempo binary                              │
│                                                               │
│  ┌──────────────────────────────────┐  ┌──────────────────┐  │
│  │       reth node (launched)       │  │   consensus       │  │
│  │                                  │  │   engine          │  │
│  │  • Database                      │  │   (commonware     │  │
│  │  • EVM                           │  │    simplex)       │  │
│  │  • Transaction pool              │  │                   │  │
│  │  • Networking / DevP2P           │  │  • P2P            │  │
│  │  • JSON-RPC (eth_, web3_)        │  │  • Simplex BFT    │  │
│  │  • Engine API (engine_)          │  │  • Block building │  │
│  │  • add_ons_handle ───────────────┼──│  • Block          │  │
│  │    .beacon_engine_handle         │  │    verification   │  │
│  └──────────────────────────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## File-by-File Architecture

### 1. `crates/node/src/node.rs` — `TempoNode` + `TempoAddOns`

This is the central file. It defines:

#### `TempoNode` (struct)

A stateless type that tells reth what primitives, chain spec, storage, and payload types to use.

```rust
pub struct TempoNode {
    pool_builder: TempoPoolBuilder,
    payload_builder_builder: TempoPayloadBuilderBuilder,
    validator_key: Option<B256>,
}
```

**`impl NodeTypes for TempoNode`** — Associates the types:

```rust
impl NodeTypes for TempoNode {
    type Primitives = TempoPrimitives;      // custom block, header, tx types
    type ChainSpec = TempoChainSpec;        // custom chain spec with epoch info
    type Storage = EmptyBodyStorage<TempoTxEnvelope, TempoHeader>;
    type Payload = TempoPayloadTypes;       // wraps EthBuiltPayload<TempoPrimitives>
}
```

**`impl Node<N> for TempoNode`** — Provides component builders:

```rust
impl<N> Node<N> for TempoNode
where
    N: FullNodeTypes<Types = Self>,
{
    type ComponentsBuilder = ComponentsBuilder<N,
        TempoPoolBuilder,                    // custom transaction pool
        BasicPayloadServiceBuilder<TempoPayloadBuilderBuilder>,
        EthereumNetworkBuilder,              // standard devp2p
        TempoExecutorBuilder,               // custom EVM config
        TempoConsensusBuilder,               // custom consensus validation
    >;
    type AddOns = TempoAddOns<N>;

    fn components_builder(&self) -> Self::ComponentsBuilder { ... }
    fn add_ons(&self) -> Self::AddOns { ... }
}
```

#### `TempoAddOns<N>` (struct)

Extensions layered on top of the base node — custom RPC modules and the engine validator.

```rust
pub struct TempoAddOns<N: FullNodeTypes<Types = TempoNode>> {
    inner: RpcAddOns<...>,
    validator_key: Option<B256>,
}
```

Implements `RethRpcAddOns` and `EngineValidatorAddOn`. The `beacon_engine_handle` lives on the `RpcHandle` returned by `RpcAddOns::launch_add_ons_with()`.

### 2. `crates/node/src/lib.rs` — Type Aliases

Defines the concrete types after launch:

```rust
type TempoFullNodeTypes = RethFullAdapter<DatabaseEnv, TempoNode>;
type TempoNodeAdapter = NodeAdapter<TempoFullNodeTypes>;

pub type TempoFullNode = FullNode<TempoNodeAdapter, TempoAddOns<TempoFullNodeTypes>>;
```

`TempoFullNode` is the launched node. It has:

```rust
pub struct FullNode<Node, AddOns> {
    pub evm_config,
    pub pool,
    pub network,
    pub provider,
    pub payload_builder_handle,
    pub task_executor,
    pub config,
    pub data_dir,
    pub add_ons_handle: AddOns::Handle,   // <-- this is RpcHandle
}
```

`AddOns::Handle` for `TempoAddOns` is `RpcHandle<NodeAdapter<N>, TempoEthApi<...>>`. The `RpcHandle` has:

```rust
pub struct RpcHandle<Node, EthApi> {
    pub rpc_server_handles,
    pub rpc_registry,
    pub engine_events,
    pub beacon_engine_handle: ConsensusEngineHandle<Payload>,  // <-- THIS
    pub engine_shutdown,
}
```

### 3. `crates/primitives/src/reth_compat/mod.rs` — `NodePrimitives` impl

Tempo's custom types implement reth's `NodePrimitives` trait:

```rust
impl NodePrimitives for TempoPrimitives {
    type Block = Block;                        // alloy_consensus::Block<TempoTxEnvelope, TempoHeader>
    type BlockHeader = TempoHeader;
    type BlockBody = BlockBody;
    type SignedTx = TempoTxEnvelope;
    type Receipt = TempoReceipt;
}
```

This feature-gated behind `#[cfg(feature = "reth")]` in `Cargo.toml`, which pulls in `reth-primitives-traits`, `reth-ethereum-primitives`, `reth-db-api`.

### 4. `crates/payload/types/src/lib.rs` — `TempoPayloadTypes`

Implements `PayloadTypes` for Tempo's custom payload:

```rust
impl PayloadTypes for TempoPayloadTypes {
    type BuiltPayload = TempoBuiltPayload;     // wraps EthBuiltPayload<TempoPrimitives>
    type PayloadAttributes = TempoPayloadAttributes;
    type ExecutionData = ExecutionData;        // from alloy_rpc_types_engine
}
```

`TempoBuiltPayload` wraps `EthBuiltPayload<TempoPrimitives>` and adds Tempo-specific data (block access lists, validation latency estimates, etc.).

### 5. `bin/tempo/src/lib.rs` — Launch Flow

The launch has two phases that run in parallel:

#### Phase A: Launch reth node (tokio runtime)

```rust
let NodeHandle { node, node_exit_future } = builder
    .node(TempoNode::new(&args, validator_key))
    .extend_rpc_modules(|ctx| { /* custom RPC */ })
    .launch_with_debug_capabilities()
    .await?;
```

After launch:
- `node` is a `TempoFullNode`
- `node.add_ons_handle.beacon_engine_handle` is available

The node handle is sent to the consensus thread via a channel:

```rust
let _ = args_and_node_handle_tx.send((node, args));
```

#### Phase B: Launch consensus engine (separate commonware tokio runtime)

In a separate OS thread with its own tokio runtime:

```rust
let consensus_handle = thread::spawn(move || {
    let (node, args) = args_and_node_handle_rx.blocking_recv()?;

    let runner = commonware_runtime::tokio::Runner::new(runtime_config);
    runner.start(async move |ctx| {
        run_consensus_stack(ctx, args.consensus, Arc::new(node), feed_state).await
    })
});
```

### 6. `crates/consensus/src/lib.rs` — `run_consensus_stack`

This is where the `beacon_engine_handle` wires into block building:

```rust
pub async fn run_consensus_stack(
    context: ...,
    config: Args,
    execution_node: Arc<TempoFullNode>,     // <-- has add_ons_handle
    feed_state: FeedStateHandle,
) -> eyre::Result<()> {
    // ...
    let engine = Builder {
        execution_node: Some(execution_node.clone()),
        // ...
    }
    .try_init(context.with_label("engine"))
    .await?;

    engine.start(votes, certs, resolver, ...).await
}
```

The `Builder` passes `execution_node` into the application actor, which uses `node.add_ons_handle.beacon_engine_handle` directly:

```rust
// In application/actor.rs:
self.execution_node.add_ons_handle.beacon_engine_handle.clone()
```

Tempo does **not** use a `PayloadBuilder` trait abstraction like allegro. Instead, the application actor calls `fork_choice_updated` and `new_payload` on the handle directly during `handle_propose()` and `handle_verify()`.

---

## Component Analysis: What Reth Provides vs What Needs Custom Code

### Can Allegro Use `EthereumNode` Directly?

**Yes.** Allegro does not need any custom node types. `EthereumNode` from `reth-node-ethereum` provides all components out of the box. Tempo only customizes them because it needs Tempo-specific features (AA transactions, custom EVM, etc.). Allegro has no such requirements.

### Component-By-Component Breakdown

| Component | Reth Default (`EthereumNode`) | Tempo Custom | Why Tempo Customizes | Allegro Needs Custom? |
|-----------|------------------------------|--------------|---------------------|-----------------------|
| **Pool** | `EthereumPoolBuilder` → standard `EthTransactionPool` | `TempoPoolBuilder` → `TempoTransactionPool` (AA 2D pool) | Tempo has Account Abstraction transactions with 2D nonce ordering | **No** — standard tx pool handles `eth_sendRawTransaction` |
| **Payload** | `EthereumPayloadBuilder` → standard block building from mempool | `TempoPayloadBuilder` with prewarming, parallel building, BSL | Tempo has custom payload attributes (timestamp_millis, validator set) | **No** — standard block building works; consensus calls `fork_choice_updated` with standard `PayloadAttributes` |
| **Network** | `EthereumNetworkBuilder` → standard devp2p | Same (`EthereumNetworkBuilder`) | No customization needed | **No** — use directly |
| **Executor** | `EthExecutorBuilder` → standard EVM | `TempoEvmConfig` | Tempo has custom precompiles and EVM configuration | **No** — standard EVM works |
| **Consensus** | `EthConsensusBuilder` → `EthBeaconConsensus` | `TempoConsensus` (custom) | Tempo validates custom header fields (BAL hashes) | **No** — `EthBeaconConsensus` validates standard PoS headers |
| **RPC/AddOns** | `RethRpcAddOns` → `RpcHandle` with `beacon_engine_handle` | `TempoAddOns` adds custom RPC modules (token, admin, operator) | Tempo has custom APIs | **No** — standard RPC provides `engine_*`, `eth_*`, `net_*` endpoints |

### Why Tempo Customizes and Allegro Doesn't

Every customization Tempo makes is for a specific Tempo feature:

- **`TempoPoolBuilder`**: Adds an "AA 2D pool" for account-abstraction transactions with separate nonce tracking. Allegro doesn't have AA.
- **`TempoPayloadBuilder`**: Adds prewarming (pre-execute transactions speculatively), parallel block planning, and block access lists (BAL). Allegro doesn't need these.
- **`TempoEvmConfig`**: Registers custom precompiles for BLS12-381, ed25519, and validator config contracts. Allegro uses standard EVM.
- **`TempoConsensus`**: Validates Tempo-specific header fields. Allegro's consensus is handled by commonware simplex.
- **`TempoPayloadTypes`**: Wraps `EthBuiltPayload` with extra fields (validation latency estimates, block access lists). Allegro can use `EthPayloadTypes` or `EthEngineTypes` directly.

### The Minimal Integration Path

1. **Use `EthereumNode` as-is** — no custom `NodeTypes`, `Node`, or `AddOns`:

```rust
use reth_node_ethereum::EthereumNode;

let handle = NodeBuilder::new(config)
    .node(EthereumNode::default())
    .launch()
    .await?;

let engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
// type: ConsensusEngineHandle<EthEngineTypes>
```

2. **No custom primitives needed** — use `EthPrimitives` (standard Ethereum block, header, tx). The `AllegroHeader` with consensus context is only for the stub builder. When reth builds blocks, they're standard Ethereum blocks. The consensus context is tracked in the application actor, not in the block header.

3. **Wire the handle directly** — the `create_reth_payload_builder` closures call `handle.fork_choice_updated()` and `handle.new_payload()` with standard types:

```rust
type Payload = EthEngineTypes; // from EthereumNode

let build = |req: BuildPayloadRequest| {
    let h = engine_handle.clone();
    Box::pin(async move {
        let state = ForkchoiceState { head: req.parent_hash, safe: req.parent_hash, finalized: req.parent_hash };
        let attrs = PayloadAttributes { timestamp: req.timestamp, ... };
        let fcu = h.fork_choice_updated(state, Some(attrs)).await?;
        // resolve payload via PayloadBuilderHandle or build empty fallback
        Ok(BuiltPayload { block_bytes, block_hash, block_number })
    })
};
```

### Summary

| Component | Source |
|-----------|--------|
| `NodeTypes` | `EthereumNode` (default) |
| `Node` impl | `EthereumNode` (default) |
| `AddOns` | `EthereumAddOns` (default) |
| Pool | `EthereumPoolBuilder` (default) |
| Payload | `EthereumPayloadBuilder` (default) |
| Network | `EthereumNetworkBuilder` (default) |
| Executor | `EthExecutorBuilder` (default) |
| Consensus | `EthConsensusBuilder` (default) |
| Primitives | `EthPrimitives` (standard) |
| Payload types | `EthEngineTypes` (standard) |
| Chain spec | `reth_chainspec::ChainSpec` (standard) |
| **Custom code** | **None** — only the `create_reth_payload_builder` closures |

### What Still Needs Custom Code

1. **The `create_reth_payload_builder` closures** — these convert between allegro's `BuildPayloadRequest` and reth's Engine API calls. This is ~50 lines of code.
2. **The launch flow** — running reth on one tokio runtime and commonware consensus on another, in two threads. This is ~100 lines of boilerplate.

Everything else — database, EVM, pool, network, RPC, engine API — comes from reth's `EthereumNode` defaults.

```toml
# allegro-primitives/Cargo.toml
[dependencies]
reth-primitives-traits = { git = "...", optional = true }
reth-ethereum-primitives = { git = "...", optional = true }

[features]
reth = ["reth-primitives-traits", "reth-ethereum-primitives"]
```

```rust
// allegro-primitives/src/reth_compat.rs
impl NodePrimitives for AllegroPrimitives {
    type Block = Block;
    type BlockHeader = AllegroHeader;
    type BlockBody = BlockBody;
    type SignedTx = TxEnvelope;
    type Receipt = AllegroReceipt;
}
```

### Layer 2: Node Types

Create `AllegroNode`:

```rust
// bin/allegro/src/node.rs
pub struct AllegroNode;

impl NodeTypes for AllegroNode {
    type Primitives = AllegroPrimitives;
    type ChainSpec = ChainSpec;
    type Storage = EthStorage;
    type Payload = EthPayloadTypes;  // or custom AllegroPayloadTypes
}

impl<N> Node<N> for AllegroNode
where
    N: FullNodeTypes<Types = Self>,
{
    type ComponentsBuilder = ComponentsBuilder<N, ...>;
    type AddOns = AllegroAddOns<N>;

    fn components_builder(&self) -> Self::ComponentsBuilder {
        EthereumNode::components()  // reuse standard components
    }
    fn add_ons(&self) -> Self::AddOns {
        AllegroAddOns::default()
    }
}
```

### Layer 3: AddOns with `beacon_engine_handle`

Create `AllegroAddOns` that holds the `ConsensusEngineHandle`:

```rust
pub struct AllegroAddOns<N: FullNodeTypes<Types = AllegroNode>> {
    inner: RpcAddOns<...>,
}

impl<N> RethRpcAddOns<NodeAdapter<N>> for AllegroAddOns<N> {
    // Standard RPC setup — eth_, web3_, net_
}

impl<N> NodeAddOns<NodeAdapter<N>> for AllegroAddOns<N> {
    type Handle = RpcHandle<NodeAdapter<N>, EthApi<NodeAdapter<N>>>;

    async fn launch_add_ons(self, ctx) -> eyre::Result<Self::Handle> {
        self.inner.launch_add_ons_with(ctx, |_| Ok(())).await
    }
}
```

After launch, `node.add_ons_handle.beacon_engine_handle` is available.

### Layer 4: Payload Builder Wiring

Two approaches:

#### A. Tempo-style (direct handle usage in application actor)

Pass `Arc<TempoFullNode>` to the consensus engine's `Builder`. The application actor accesses `self.execution_node.add_ons_handle.beacon_engine_handle` directly.

#### B. Allegro-style (via `PayloadBuilder` trait + `create_reth_payload_builder`)

```rust
let handle = node.add_ons_handle.beacon_engine_handle.clone();

let builder = create_reth_payload_builder(
    move |req: BuildPayloadRequest| {
        let h = handle.clone();
        Box::pin(async move {
            let (state, attrs) = build_payload_attributes_from_request(&req);
            let response = h.fork_choice_updated(state, Some(attrs)).await?;
            // resolve payload_id -> BuiltPayload
        })
    },
    move |req: ValidateBlockRequest| {
        let h = handle.clone();
        Box::pin(async move {
            let status = h.new_payload(req.block_bytes, None).await?;
            // status -> ValidationResult
        })
    },
);
```

### Layer 5: Launch Flow

```rust
// In main():
let NodeHandle { node, node_exit_future } = builder
    .node(AllegroNode)
    .launch()
    .await?;

// Pass node to consensus engine thread
let engine_handle = node.add_ons_handle.beacon_engine_handle.clone();

let (payload_builder, tx_pool) = create_payload_builder(engine_handle);

// Start consensus engine with payload_builder on separate commonware runtime
thread::spawn(move || {
    let runner = commonware_runtime::tokio::Runner::new(config);
    runner.start(async move |ctx| {
        // wire payload_builder into start_simplex_engine
    })
});
```

---

## Dependencies Required

Dependencies to add to `bin/allegro/Cargo.toml`:

```toml
reth-chainspec = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-db = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-engine-primitives = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-ethereum = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384", features = ["node"] }
reth-ethereum-engine-primitives = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-node-builder = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-node-core = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-node-ethereum = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-payload-builder = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-payload-primitives = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-primitives-traits = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
reth-provider = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
```

And in `allegro-primitives/Cargo.toml`:

```toml
[dependencies]
reth-primitives-traits = { git = "...", optional = true }
reth-ethereum-primitives = { git = "...", optional = true }

[features]
reth = ["reth-primitives-traits", "reth-ethereum-primitives"]
```

---

## Key Differences from Current State

| Aspect | Current (allegro) | Target (tempo-style) |
|--------|-------------------|---------------------|
| Binary type | Standalone consensus node | Reth node embedding consensus |
| Payload builder | Stub (empty blocks) | `ConsensusEngineHandle` via `fork_choice_updated` |
| Transaction pool | None | Reth's transaction pool |
| JSON-RPC | None | Reth's standard `eth_*` + `engine_*` endpoints |
| Database | None | `reth-db` (mdbx) |
| Block building | Empty blocks via `build_empty_block_internal` | Reth's payload builder service |
| Block validation | Structural check only | `engine_newPayload` via consensus engine handle |
| Runtime | Commonware tokio only | Commonware tokio + reth tokio (separate threads) |

---

## Implementation Status

As of the integration plan (`docs/reth-integration-plan.md`), the implementation is complete across all 6 phases:

| Phase | Status | Summary |
|-------|--------|---------|
| 0 — Cleanup | ✅ | Replaced `crates/node` with `allegro-node`; added workspace deps for reth |
| 1 — Consensus extensions | ✅ | `BlockMeta`, `BlockInfo.timestamp`, verified-block-info recording, genesis hash, finalization sink, `StartedEngine` |
| 2 — Payload attrs fix | ✅ | `parent_beacon_block_root: Some(ZERO)` for Cancun+ |
| 3 — Real reth crate | ✅ | `allegro-node`: chainspec, launch (DB open + NodeBuilder), builder closures (FCU → resolve → self-import), finalizer (FCU on finalization) |
| 4 — Binary wiring | ✅ | `--execution reth|stub` CLI (default `reth`), dual-thread dual-runtime launch |
| 5 — Tests | ✅ | Chainspec unit tests; engine roundtrip test skeleton (ignored — hangs on node launch) |
| 6 — Scripts & docs | ✅ | `run_devnet.sh` reth mode, README update, this status section |

### Key design deviations from the original analysis

1. **`ConsensusEngineHandle` uses `EthEngineTypes`**, not `EthPayloadTypes` (EthereumNode's `Payload` type is `EthEngineTypes`).
2. **EthereumNode constructor has private fields** — must use `EthereumNode::default()`.
3. **`reth_db::DatabaseArguments` is not publicly re-exported** — use `reth_db::mdbx::DatabaseArguments`.
4. **Genesis hash/timestamp** accessed via `ChainSpec::genesis_hash()` and `genesis_timestamp()` methods.
5. **`TaskExecutor = Runtime`** — the reth runtime handle IS the task executor; pass `runner.runtime()` directly.

### Remaining issues

- **Engine roundtrip test hangs** — in-process reth node launch times out (port binding / Runtime conflict). Workaround: manual devnet testing via `run_devnet.sh`.
- **No block sync** — nodes that miss a finalized block's verification cannot backfill. FCU returns `SYNCING`. Out of scope for v1.
- **No tx pool gossip** — devp2p peering between reth sub-nodes is not configured. Txs must be submitted to the node that will propose.
