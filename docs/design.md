# Allegro Design Document

> Consensus-and-execution node embedding commonware simplex BFT consensus with a reth
> EVM execution layer.  Two runtimes, one binary.

## 1. Overview

Allegro is a blockchain node where **consensus** (commonware simplex) and **execution**
(reth) run as peers in the same process, communicating through reth's Engine API channel
handles rather than through JSON-RPC over localhost.

```
┌──────────────────────────── allegro process ────────────────────────────┐
│                                                                         │
│  Thread 1 (reth tokio)                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  EthereumNode                                                    │   │
│  │  ┌──────┐ ┌──────┐ ┌────────┐ ┌──────────┐ ┌──────────────┐     │   │
│  │  │  DB  │ │ EVM  │ │ TxPool │ │ devp2p   │ │ JSON-RPC     │     │   │
│  │  │ mdbx │ │      │ │        │ │ (disc v4) │ │ eth_, net_   │     │   │
│  │  └──────┘ └──────┘ └────────┘ └──────────┘ └──────────────┘     │   │
│  │                                                                   │   │
│  │  payload_builder_handle   ConsensusEngineHandle                   │   │
│  │         │                         │                               │   │
│  └─────────┼─────────────────────────┼───────────────────────────────┘   │
│            │                         │                                    │
│  ══════════╪═════════════════════════╪════════════════════════════════    │
│            │   (unbounded channels)  │                                    │
│  ══════════╪═════════════════════════╪════════════════════════════════    │
│            │                         │                                    │
│  Thread 2 (commonware tokio)         │                                    │
│  ┌─────────┼─────────────────────────┼───────────────────────────────┐   │
│  │  ┌──────▼─────────────────────────▼──────┐                        │   │
│  │  │  EngineApiPayloadBuilder              │                        │   │
│  │  │                                       │                        │   │
│  │  │  build:  FCU(parent, attrs)           │                        │   │
│  │  │          → payload_id                 │                        │   │
│  │  │          → resolve(WaitForPending)     │                        │   │
│  │  │          → new_payload (self-import)  │                        │   │
│  │  │                                       │                        │   │
│  │  │  validate: decode → block_to_payload  │                        │   │
│  │  │            → new_payload              │                        │   │
│  │  └───────────────────────────────────────┘                        │   │
│  │                   │                                               │   │
│  │  ┌────────────────▼──────────────────────┐                        │   │
│  │  │  Simplex Engine                       │                        │   │
│  │  │  ┌──────────────┐  ┌──────────────┐   │                        │   │
│  │  │  │  Application  │  │  BlockRelay  │   │                        │   │
│  │  │  │  Actor        │  │              │   │                        │   │
│  │  │  │  propose()    │  │  broadcast() │   │                        │   │
│  │  │  │  verify()     │  └──────┬───────┘   │                        │   │
│  │  │  │  genesis()    │         │           │                        │   │
│  │  │  └──────────────┘         │           │                        │   │
│  │  │                           │           │                        │   │
│  │  │  ┌────────────────────────▼────────┐  │                        │   │
│  │  │  │  Finalizer task                 │  │                        │   │
│  │  │  │  on Finalization cert → FCU     │  │                        │   │
│  │  │  └─────────────────────────────────┘  │                        │   │
│  │  └───────────────────────────────────────┘                        │   │
│  │                                                                   │   │
│  │  ┌──────────────────────────────────┐                              │   │
│  │  │  P2P layer (lookup network)      │                              │   │
│  │  │  votes | certs | resolver | blks │                              │   │
│  │  └──────────────────────────────────┘                              │   │
│  └───────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Crate Layout

```
crates/
├── primitives/               allegro-primitives
│   └── AllegroHeader         alloy_consensus::Header + consensus context
│   └── Digest                B256 wrapper for commonware
│   └── ProposerKey, Block
│
├── consensus/                allegro-consensus
│   ├── application.rs        Application actor + Mailbox (Automaton impl)
│   ├── engine.rs             start_simplex_engine, BlockRelay, Reporter
│   ├── executor.rs           PayloadBuilder trait + StubPayloadBuilder
│   │                         + EngineApiPayloadBuilder (closure-based)
│   │                         + attrs helpers (build_payload_attributes, …)
│   ├── block.rs              Commonware codec for wire blocks
│   ├── config.rs             Tunable consensus parameters
│   ├── validators.rs         Ed25519 validator set
│   └── metrics.rs            Counters
│
├── node/                     allegro-node       (reth integration)
│   ├── launch.rs             NodeBuilder + EthereumNode launch + type aliases
│   ├── builder.rs            Production payload builder closures
│   ├── finalizer.rs          Finalization → forkchoice forwarder
│   └── chainspec.rs          DEV chainspec
│
├── bin/allegro/
│   └── main.rs               CLI, dual-runtime orchestration
│
└── xtask/
    └── main.rs               Genesis generation (Anvil mnemonic, all forks at 0)
```

### Dependency graph

```
allegro-primitives                ← Consensus types, zero reth deps

allegro-consensus                 ← Simplex wiring, PayloadBuilder trait
    ├── allegro-primitives
    └── commonware-*              ← Simplex engine, p2p, storage

allegro-node                      ← Real reth integration
    ├── allegro-consensus
    └── reth-*                    ← EthereumNode, engine primitives, DB

allegro (binary)                  ← Orchestration
    ├── allegro-node
    └── reth-cli-runner           ← Tokio runtime management
```

---

## 3. Simplex Consensus Engine

### 3.1 Engine Bootstrap (`start_simplex_engine`)

The function wires together all consensus components and returns a `StartedEngine`:

```rust
pub struct StartedEngine {
    pub task: Handle<()>,
    pub block_info: BlockInfoMap,  // digest → (hash, number, timestamp, view, proposer)
}
```

Signals **out** from the engine arrive via the `finalized_tx` channel on
`EngineConfig`, populated by the `MetricsReporter` whenever a finalization
certificate is observed.

### 3.2 Application Actor (`application.rs`)

The actor implements `commonware_consensus::Automaton` via a `Mailbox` proxy
that forwards all engine callbacks into an mpsc channel processed by the `Actor`.

| Callback    | Trigger                      | Delegates to                 |
|-------------|------------------------------|------------------------------|
| `genesis`   | Engine starts epoch 0        | Returns `Digest::EMPTY`; initialises `BlockInfo` for height 0 |
| `propose`   | This node is leader          | `PayloadBuilder::build_payload(…)` → stores bytes, records `BlockInfo` |
| `verify`    | Non-leader receives proposal | `PayloadBuilder::validate_block(…)` → records `BlockInfo` on success |

The actor maintains three shared stores:

- **`PendingBlocks`** — blocks this node proposed (so the relay can broadcast them).
- **`ReceivedBlocks`** — blocks received from peers via the P2P block channel.
- **`BlockInfoMap`** — per-digest metadata (hash, number, timestamp, view, proposer).
  Used by `propose` to find the parent block and by the `finalizer` to look up
  the canonical hash for a finalized digest.

### 3.3 Block Propagation

Proposed blocks travel over a dedicated P2P channel (index 3) via a `BlockRelay`
that implements commonware's `Relay` trait.  The wire format is a 32-byte
digest followed by the RLP-encoded block.

A `block_receiver` task spawned at engine startup listens on the same channel
and inserts incoming blocks into `ReceivedBlocks`.

### 3.4 Finalization Sink

The `MetricsReporter` (which wraps the standard `TracingReporter`) holds an
optional `mpsc::Sender<AllegroDigest>`.  On `Activity::Finalization(cert)` it
`try_send`s the digest; the receiver side runs in the `finalizer` task
(see §4.4).

### 3.5 Deterministic Tests

Consensus integration tests use `commonware_runtime::deterministic::Runner` with
`SimNetwork` for instant, reproducible multi-node simulations.  They supply the
empty-block payload builder from `tests/common` because a real reth node cannot
run inside the deterministic runtime.

---

## 4. Reth Integration

### 4.1 Why `EthereumNode` (no custom types)

Allegro requires **no** custom `NodeTypes`, `Primitives`, `Payload`, or
`AddOns`.  The standard `EthereumNode` provides everything:

| Component  | Default                    | Reason                          |
|------------|----------------------------|---------------------------------|
| Pool       | `EthTransactionPool`       | Standard `eth_sendRawTransaction` |
| Payload    | `EthereumPayloadBuilder`   | Build through FCU + attrs       |
| Network    | `EthereumNetworkBuilder`   | Discovery disabled (see below)  |
| Executor   | `EthEvmConfig`             | Standard EVM, no custom precompiles |
| Consensus  | `EthBeaconConsensus`       | PoS header validation           |

This is in contrast to Tempo, which replaces every layer with custom
implementations for account-abstraction transactions, BLS precompiles,
parallel payload building, and custom header validation.

### 4.2 Node Launch (`launch.rs`)

```rust
pub struct RethNodeConfig {
    pub datadir:    PathBuf,
    pub http_port:  u16,
    pub authrpc_port: u16,
    pub p2p_port:   u16,
    pub chain:      Arc<ChainSpec>,
}

pub struct LaunchedRethNode {
    pub engine_handle:  ConsensusEngineHandle<EthEngineTypes>,
    pub payload_builder_handle: PayloadBuilderHandle<EthEngineTypes>,
    pub genesis_hash:   B256,
    pub genesis_timestamp: u64,
    pub node:           AllegroFullNode,   // must survive—keeps RPC servers alive
    pub exit:           Pin<Box<dyn Future<…> + Send>>,
}
```

Key decisions:

- **No dev mode.** `NodeConfig::new(spec)` without `.dev()` prevents reth's
  `LocalMiner` from auto-producing blocks that would conflict with
  consensus-driven building.
- **Discovery disabled.** `DiscoveryArgs { disable_discovery: true }`.
  Allegro's embedded reth nodes do not peer with each other—blocks flow over
  the commonware consensus p2p network.  This also prevents discv4/discv5 UDP
  port collisions between co-located nodes.
- **IPC disabled.** `RpcServerArgs { ipcdisable: true }` avoids
  `/tmp/reth.ipc` conflicts.
- **Node must survive.** The `FullNode` (stored in the `node` field) holds
  jsonrpsee `ServerHandle`s; dropping it kills the HTTP and auth RPC servers
  even though the engine and p2p tasks continue running.

### 4.3 Payload Builder (`builder.rs`)

`create_engine_payload_builder(engine, payloads, tracker)` returns an
`EngineApiPayloadBuilder` with two closures:

**Build closure** (called when this node is the leader):

```
1. build_payload_attributes_from_request(&req)
       → PayloadAttributes { timestamp, prev_randao=0,
             withdrawals=Some(vec![]), parent_beacon_block_root=Some(ZERO),
             slot_number=None, target_gas_limit=None }

2. engine.fork_choice_updated(fcs, Some(attrs))
       → payload_id

3. payloads.resolve_kind(payload_id, PayloadKind::WaitForPending)
       → EthBuiltPayload

4. engine.new_payload(exec_data)          // self-import (CL pattern)
       → Valid | Accepted

5. alloy_rlp::encode(block)
       → BuiltPayload { block_bytes, block_hash, block_number }
```

**Validate closure** (called for every received proposal):

```
1. alloy_rlp::decode::<Block<TxEnvelope, Header>>(&bytes)
       → standard Ethereum block

2. EthPayloadTypes::block_to_payload(sealed, None)
       → ExecutionData (with correct sidecar version)

3. engine.new_payload(exec_data)
       → Valid(BlockMeta) | Invalid(reason) | Syncing
```

The build closure sets `parent_beacon_block_root: Some(B256::ZERO)` and
`withdrawals: Some(vec![])` because the DEV chainspec activates Cancun at
genesis.  `slot_number: None` and `block_access_list: None` because
Amsterdam is not activated.

### 4.4 Finalization Forwarder (`finalizer.rs`)

```rust
pub fn spawn_finalizer(ctx, rx, block_info, engine, tracker)
```

A background task that drains the `finalized_tx` channel from the consensus
reporter.  For each finalized digest:

1. Look up the canonical block hash from `BlockInfoMap`.
2. Submit `fork_choice_updated(head=safe=finalized=hash, None)`.
3. On success, update `ForkchoiceTracker` so subsequent build closures use the
   last-finalized hash as the forkchoice safe/finalized anchor.

**Known limitation**: if the node never called `new_payload` on the finalized
block (e.g. it was partitioned during verification), the FCU returns
`SYNCING` and is skipped.  A full backfill syncer is future work (§8).

---

## 5. Data Flow

### 5.1 Block Production

```
                  ┌──────────────┐
                  │  Simplex:    │
                  │  propose()   │
                  └──────┬───────┘
                         │ BuildPayloadRequest
                  ┌──────▼───────┐
                  │  Build       │  1. FCU(parent, attrs)
                  │  closure     │  2. resolve(payload_id)
                  │              │  3. new_payload (self-import)
                  │              │  4. RLP-encode
                  └──────┬───────┘
                         │ BuiltPayload { block_bytes, block_hash, number }
                  ┌──────▼───────┐
                  │  Actor:      │
                  │  store in    │
                  │  PendingBlks │  + record BlockInfo
                  │  → response  │
                  └──────┬───────┘
                         │ digest
                  ┌──────▼───────┐
                  │  Simplex:    │
                  │  broadcast   │  votes channel → peers
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │  BlockRelay  │  blocks channel → peers
                  └──────────────┘
```

### 5.2 Block Verification (on a non-leader peer)

```
   Peer's BlockRelay
    ┌──────┐
    │ recv │ → block bytes → ReceivedBlocks
    └──────┘
                         │
   Peer's Simplex        │ proposal message
    ┌──────────────┐     │ (votes channel)
    │  verify(dig) │─────┘
    └──────┬───────┘
           │
    ┌──────▼───────┐
    │  Actor:      │
    │  lookup      │  ReceivedBlocks[digest] or PendingBlocks[digest]
    │  block bytes │
    └──────┬───────┘
           │ ValidateBlockRequest
    ┌──────▼───────┐
    │  Validate    │  1. Decode standard Ethereum block
    │  closure     │  2. block_to_payload → ExecutionData
    │              │  3. new_payload(exec_data)
    │              │  4. → Valid(BlockMeta)
    └──────┬───────┘
           │ true/false
    ┌──────▼───────┐
    │  Simplex:    │  notarize or nullify
    └──────────────┘
```

### 5.3 Finalization

```
   Simplex Engine
    ┌────────────────────────┐
    │  Activity::Finalization │
    │  (cert)                 │
    └───────────┬─────────────┘
                │ Reporter.try_send(digest)
    ┌───────────▼─────────────┐
    │  finalizer task         │
    │                         │  1. BlockInfoMap[digest] → hash
    │                         │  2. FCU(h=s=f=hash, None)
    │                         │  3. tracker.set_finalized(hash, n)
    └───────────┬─────────────┘
                │
    ┌───────────▼─────────────┐
    │  reth engine tree       │
    │  canonical head         │  unmoved from block N-1 to N
    └─────────────────────────┘
```

### 5.4 Transaction Flow

```
   User
    │  cast send …
    ▼
   JSON-RPC (HTTP :8545)
    │  eth_sendRawTransaction
    ▼
   reth TransactionPool
    │
    ▼
   On next proposer turn:
   Build closure → FCU(attrs) → pool content sealed into block
    │
    ▼
   Consensus: propose → verify → finalize
    │
    ▼
   Finalizer FCU → block N committed to canonical chain
    │
    ▼
   eth_getTransactionReceipt returns the mined tx
```

**Multi-node caveat**: reth nodes are not peering with each other (discovery
disabled).  A transaction sent to node A's RPC only enters node A's pool.  It
gets included when node A is the leader (round-robin guarantees this within N
views).  Tx gossip via devp2p peering is future work (§8).

---

## 6. P2P Network Architecture

### 6.1 Consensus P2P

Uses commonware's `lookup::Network` with Ed25519 identity keys derived from
`PrivateKey::from_seed(node_index)`.  Four multiplexed channels:

| Index | Purpose       | Rate limit |
|-------|---------------|------------|
| 0     | Votes         | 128 / s    |
| 1     | Certificates  | 128 / s    |
| 2     | Resolver      | 128 / s    |
| 3     | Blocks        | 128 / s    |

Peer addresses are exchanged through the validator set constructed from CLI
`--consensus.node-index`/`--consensus.peer` arguments.  Both sides must track each other—a missing
`--consensus.peer` on one node causes the other's connection to be rejected by the
bouncer callback (`tracker.acceptable`).

### 6.2 Reth P2P

Discovery is disabled (`disable_discovery: true`).  The TCP listener binds to
`--reth-p2p-port` for potential future peering (tx gossip), but currently no
connections are established.

### 6.3 Chronology of a Connection

```
Node A                         Node B
  │                              │
  │ network.start()              │ network.start()
  │ listener: accept()           │ dialer: resolve + connect
  │                              ├────────────────────────────►
  │ ◄────────────────────────────┤ TCP handshake
  │                              │
  │ stream::listen()             │ stream::dial()
  │   recv public key            │   send public key
  │   bouncer(peer)              │   send SYN
  │   recv SYN                   │   recv SYN-ACK
  │   listen_start (timestamps)  │   send ACK
  │   send SYN-ACK               │   ✓ connected
  │   recv ACK                   │
  │   ✓ completed handshake      │
  │                              │
  │ ◄══════════════════════════►│ Encrypted stream up
```

Key configuration:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `synchrony_bound` | 2 s | Clock skew tolerance |
| `max_handshake_age` | 300 s | Generous replay protection window |
| `handshake_timeout` | 5 s | Per-handshake deadline |
| `bypass_ip_check` | true | Devnet: peers on localhost with different IPs |
| `allow_private_ips` | true | Devnet |

---

## 7. Key Design Decisions

### 7.1 Standard Ethereum Blocks as the Consensus Payload

**Decision**: consensus carries RLP-encoded standard `Block<TxEnvelope, Header>`
bytes; `AllegroConsensusContext` (epoch, view, proposer) is tracked in the
application actor's `BlockInfoMap`, NOT in the block header.

**Why**: reth builds standard blocks.  Embedding consensus metadata in the
header would require a custom `NodePrimitives` type (breaking EVM compatibility,
requiring custom RPC types, and duplicating every reth component).  Tempo
accepts this cost because it needs AA transactions and BLS precompiles;
Allegro does not.

### 7.2 Dual-Tokio-Runtime

Consensus runs on a `commonware_runtime::tokio::Runner` in a dedicated OS
thread; reth runs on a `reth_tasks::Runtime` obtained from
`reth_cli_runner::CliRunner` on the main thread.  Both runtimes are
independent.

Engine API handles (`ConsensusEngineHandle`, `PayloadBuilderHandle`) are
**cheap cloneable channel senders** (unbounded mpsc + oneshot).  They can be
moved into and `.await`ed from either runtime.

### 7.3 Cross-Runtime Communication

```
Main thread (reth runtime)         Consensus thread (commonware runtime)
──────────────────────────         ──────────────────────────────────────
1. launch reth                     1. block on channel
2. extract handles                 2. receive handles
3. send handles via sync channel   3. setup p2p
4. await ctrl-c or node exit       4. spawn finalizer
                                   5. start simplex engine
                                   6. loop { sleep(3600) }
```

The sync channel (std mpsc) bridges the two threads; the handles themselves
carry the tokio-agnostic channel halves.

### 7.4 No Custom RPC Modules

Allegro exposes reth's standard JSON-RPC surface: `eth_*`, `net_*`, `web3_*`.
No consensus status endpoint is added (compared to Tempo's
`tempo_consensusBlock`).  For devnets, `eth_blockNumber` is sufficient.

### 7.5 Timestamp Rule

```rust
let timestamp = std::cmp::max(now_secs, parent_timestamp + 1);
```

Required by the Engine API spec: `PayloadAttributes.timestamp` must be
strictly greater than `parent_header.timestamp`.  This limits block rate to
one per second maximum, which is acceptable for the target use case.

### 7.6 Clean Shutdown

Sending `SIGINT` / `SIGTERM` triggers the reth runtime's graceful shutdown
via `CliRunner::block_on`.  The consensus thread is joined with a timeout;
unclean termination is acceptable for the current development phase.

---

## 8. Known Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| No block sync / backfill | Node that missed verification of block N cannot finalize it (FCU returns SYNCING) | v2: implement backfill syncer |
| No tx gossip between reth nodes | Tx sent to node A's RPC only enters node A's pool | v2: static reth peering |
| In-memory `BlockInfoMap` | Lost on restart; parent lookups fail → consensus stalls | v2: persist or rebuild from marshal journal |
| 1 block/second max rate | Simplex can propose faster than timestamp rule allows | Acceptable; v2: millisecond timestamps via custom payload attributes (like Tempo) |
| Tracing subscriber conflict | Consensus engine logs sometimes not captured in devnet script | Investigate reth tracing initialisation order |
| Binary uses hardcoded DEV chainspec | `xtask genesis` output not yet wired into the binary | v2: `--genesis` flag reads xtask JSON |

---

## 9. API Reference

### 9.1 Consensus Engine Handle

```rust
// reth_engine_primitives::ConsensusEngineHandle<EthEngineTypes>
impl ConsensusEngineHandle {
    pub async fn fork_choice_updated(
        &self,
        state: ForkchoiceState,
        payload_attrs: Option<PayloadAttributes>,
    ) -> Result<ForkchoiceUpdated, BeaconForkChoiceUpdateError>;

    pub async fn new_payload(
        &self,
        payload: ExecutionData,
    ) -> Result<PayloadStatus, BeaconOnNewPayloadError>;
}
```

### 9.2 Payload Builder Handle

```rust
// reth_payload_builder::PayloadBuilderHandle<EthEngineTypes>
impl PayloadBuilderHandle {
    pub async fn resolve_kind(
        &self,
        id: PayloadId,
        kind: PayloadKind,     // WaitForPending | Earliest
    ) -> Option<Result<EthBuiltPayload, PayloadBuilderError>>;
}
```

### 9.3 Payload Builder Trait (consensus crate)

```rust
pub trait PayloadBuilder: Send + Sync {
    fn build_payload(
        &self,
        parent_hash: B256,
        parent_number: u64,
        parent_view: u64,
        parent_digest: AllegroDigest,
        epoch: u64,
        view: u64,
        proposer: [u8; 32],
        timestamp: u64,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>>;

    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
        parent_hash: B256,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>>;
}
```

### 9.4 Consensus Engine Bootstrap

```rust
pub fn start_simplex_engine<TContext, …>(
    context: TContext,
    config: EngineConfig,
    votes: (Sender, Receiver),
    certs: (Sender, Receiver),
    resolver: (Sender, Receiver),
    block_sender: Sender,
    block_receiver: Receiver,
    blocker: Blocker,
) -> Result<StartedEngine, ConsensusError>;
```

### 9.5 CLI

```
allegro node [reth flags] [--consensus.* flags]   # reth's CLI + consensus extension
allegro node --dev [reth flags]                   # solo-validator devnet (dev chain, no genesis file)

# consensus flags: --consensus.node-index, --consensus.listen-address,
# --consensus.peer (must appear on every node for each peer),
# --consensus.leader-timeout, --consensus.cert-timeout, ...
```

---

## 10. Devnet Genesis Generator (`xtask`)

The `allegro-xtask` crate generates devnet artefacts for a local N-validator
network:

```bash
cargo run -p allegro-xtask -- genesis \
    --validators 4 \
    --base-port 13000 \
    --chain-id 1337 \
    --output ./devnet
```

### Output files

| File | Contents |
|------|----------|
| `genesis.json` | Full `alloy_genesis::Genesis` with `ChainConfig`, alloc, nonce |
| `validators.json` | Per-validator index, Ed25519 public key, p2p address |

### Genesis configuration

- **Chain ID** — configurable (default 1337).
- **Funded accounts** — 20 accounts derived from the Anvil mnemonic
  (`"test test … junk"`) via BIP-44 `m/44'/60'/0'/0/{i}`, each with 10 000 ETH.
  This matches `cast` / forge defaults.
- **Hard forks** — all block-number forks at `0`, Paris TTD = 0,
  Shanghai/Cancun/Prague/Osaka at timestamp 0.  Amsterdam NOT activated.
- **Gas limit** — 30 000 000.

### Validator keys

Validators use Ed25519 keys derived from `PrivateKey::from_seed(i as u64)`,
which is exactly what the allegro binary does when given `--consensus.node-index <i>`.
No key files are generated: the binary derives the same keys deterministically from the validator index, so no key material needs to be shared.

---

## 11. Devnet Quickstart

```bash
# Build and run a 2-node devnet
./run_devnet.sh 2 13000 reth

# Node 0 RPC at http://127.0.0.1:8545
# Node 1 RPC at http://127.0.0.1:8546

# Send a transaction (needs `cast` from foundry)
cast send --rpc-url http://127.0.0.1:8545 \
    --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
    0x000000000000000000000000000000000000dEaD --value 0.01ether

# Check block number
cast block-number --rpc-url http://127.0.0.1:8545
```

---

## 12. Future Work

- **Block backfill / syncer**: nodes that miss block verification should fetch
  blocks from peers and call `new_payload` retroactively.
- **Static reth peering**: connect embedded reth nodes via trusted devp2p peers
  so transactions propagate to all pools, not just the receiving node's.
- **Custom genesis**: wire the xtask-generated genesis JSON into a reth
  `ChainSpec` for non-DEV devnets.
- **Persistent `BlockInfoMap`**: rebuild from the marshal journal on restart,
  or persist alongside consensus storage.
- **Millisecond timestamps**: custom payload attributes (like Tempo) to lift
  the 1 block/second limit.
- **Consensus RPC endpoint**: expose current view/epoch/peers for operational
  visibility.
- **Production chainspec**: Ethereum mainnet or Sepolia support with properly
  configured TTD, deposit contract, and blob schedule.
