# Allegro × Reth 真实集成实施计划

> 前置文档：`docs/reth-integration-analysis.md`（架构分析，已确认结论：Allegro 直接使用标准
> `EthereumNode`，无需自定义 NodeTypes / Primitives / AddOns）。
>
> 本文档是**可执行**的实施计划。所有 reth API 结论均已对照
> `~/.cargo/git/checkouts/reth-e231042ee7db3fb7/1bf2384`（rev `1bf2384`）源码核实。

---

## 0. 现状与差距

### 已有

| 组件 | 位置 | 状态 |
|---|---|---|
| `PayloadBuilder` trait + `StubPayloadBuilder`（空块） | `crates/consensus/src/executor.rs` | ✅ 可用 |
| `EngineApiPayloadBuilder`（闭包式）+ `create_reth_payload_builder` | `crates/consensus/src/executor.rs`、`crates/reth/src/payload.rs` | ✅ 骨架可用，但 attrs 不合法（见 §3.4） |
| simplex engine + application actor + BlockInfoMap | `crates/consensus/src/engine.rs`、`application.rs` | ✅ 可用，缺 3 个扩展点（见 §3.2） |
| `crates/node`（allegro-node） | `crates/node/` | ❌ 废弃：不在 workspace members；引用未声明依赖；`EmptyBodyStorage<_, AllegroHeader>` 与"标准 primitives"决策冲突 |

### 核心差距

1. **没有真实 reth 节点启动流程**（bin 只用 commonware runtime，无 reth tokio runtime、无 NodeBuilder 调用）。
2. **没有 finalization → forkchoice 通道**：reth 永远收不到 FCU(finalized)，链不会推进，`eth_getBlockByNumber` 永远停在 genesis。
3. **BlockInfoMap 只在 propose 时写入**：节点验证他人区块后不记录 (number, hash, timestamp)，轮到自己 propose 时查不到 parent → 回退 `(0, B256::ZERO)`。stub 模式无害，reth 模式致命。
4. **payload attributes 不合法**：`build_payload_attributes` 缺 `parent_beacon_block_root`（Cancun+ 必须 Some）、timestamp 必须严格大于 parent（`attr.timestamp() <= parent.timestamp()` → `INVALID_PAYLOAD_ATTRIBUTES`）。
5. **genesis 映射**：共识 genesis digest 是 `Digest::EMPTY`，reth FCU head 必须是 chainspec 的 genesis hash。

---

## 1. 目标架构

```
┌──────────────────────────── allegro 进程 ────────────────────────────┐
│                                                                      │
│  主线程 (reth tokio runtime, 经 CliRunner/reth_tasks::Runtime)        │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ EthereumNode (reth 默认组件，零自定义)                          │   │
│  │  DB(mdbx) / EVM / TxPool / devp2p / JSON-RPC(eth_,net_,web3_) │   │
│  │  add_ons_handle.beacon_engine_handle ──────┐                  │   │
│  │  payload_builder_handle ───────────────────┤                  │   │
│  └────────────────────────────────────────────┼──────────────────┘   │
│                                               │ (channel+oneshot,    │
│  共识线程 (commonware tokio runtime)           │  跨 runtime 安全)     │
│  ┌────────────────────────────────────────────┼──────────────────┐   │
│  │ simplex engine                              ▼                  │   │
│  │  application actor ──► EngineApiPayloadBuilder 闭包            │   │
│  │    ├ propose: FCU(parent, attrs) → payload_id                  │   │
│  │    │          → resolve_kind(WaitForPending) → block           │   │
│  │    │          → new_payload(block)  // 自导入                   │   │
│  │    └ verify:  decode → block_to_payload → new_payload          │   │
│  │  reporter ──► finalized_tx ──► finalizer task                  │   │
│  │    └ FCU(head=safe=finalized=block_hash)                       │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

**crate 布局**（改动范围）：

| crate | 改动 |
|---|---|
| `allegro-primitives` | 不动（`AllegroHeader`/`Digest` 保留给 stub 路径） |
| `allegro-consensus` | 小改：genesis hash、BlockInfo 加 timestamp、verify 时记录 BlockInfo、finalization sink、`StartedEngine` 返回值 |
| `allegro-reth` | 保持轻量（不引 reth 重依赖）：修正 attrs 构造；保留闭包构造器供测试 |
| `allegro-node`（重建 `crates/node`） | **新集成 crate**：chainspec、节点启动、payload builder 闭包实现、finalizer。加入 workspace members |
| `bin/allegro` | 接线：`--execution stub|reth`、双 runtime 启动、关闭流程 |

---

## 2. 已核实的 reth API 事实（rev 1bf2384）

1. `node.add_ons_handle.beacon_engine_handle: ConsensusEngineHandle<EthPayloadTypes>`
   - `fork_choice_updated(ForkchoiceState, Option<PayloadAttributes>) → Result<ForkchoiceUpdated, BeaconForkChoiceUpdateError>`（`ForkchoiceUpdated.payload_id: Option<PayloadId>`）
   - `new_payload(ExecutionData) → Result<PayloadStatus, BeaconOnNewPayloadError>`
   - 内部是 unbounded channel + oneshot，**可从任意 tokio runtime await**（commonware runtime 里直接 `.await` 安全）。
2. `node.payload_builder_handle: PayloadBuilderHandle<EthPayloadTypes>`
   - `resolve_kind(PayloadId, PayloadKind) → Option<Result<EthBuiltPayload, PayloadBuilderError>>`
   - `PayloadKind::WaitForPending`：等 pending job 出最佳 payload；`Earliest`：竞速空块 job，最快返回。
3. `EthPayloadTypes`: `BuiltPayload = EthBuiltPayload`，`PayloadAttributes = alloy_rpc_types_engine::PayloadAttributes`（别名），`ExecutionData = alloy_rpc_types_engine::ExecutionData`。
4. `EthBuiltPayload::block() → &SealedBlock<Block>`；`EthPayloadTypes::block_to_payload(SealedBlock, Option<Bytes>) → ExecutionData`（`PayloadTypes` trait 方法，负责正确构造 sidecar）。
5. **FCU attrs 校验**（`reth-engine-primitives` 默认实现）：`attr.timestamp() <= parent_header.timestamp()` → `InvalidTimestamp`。⇒ 必须 `timestamp = max(now, parent_ts + 1)`。
6. **版本字段校验**（`validate_version_specific_fields`）：Cancun+ 要求 `withdrawals: Some`、`parent_beacon_block_root: Some`；Amsterdam 才要求 `slot_number` / BAL（DEV spec 未激活 Amsterdam ⇒ 必须为 `None`）。
7. `DEV` chainspec（`reth_chainspec::DEV`）：Paris TTD=0，Shanghai/Cancun/Prague/Osaka 全部 timestamp 0 激活（**不含 Amsterdam**），含 20 个预注资账户（anvil 助记词 "test test … junk"）。
8. **禁止 dev auto-mining**：`NodeConfig.dev()` / `NodeConfig::test().dev()` 会在 launch 时启动 `LocalMiner` 自动出块（`crates/node/builder/src/launch/debug.rs:303`），与共识驱动出块冲突 ⇒ 用 `NodeConfig::new(spec)`，**不**设置 dev。
9. 启动入口：`NodeBuilder::new(config).with_launch_context(task_executor).node(EthereumNode).launch().await → NodeHandle<FullNode>`；`task_executor` 来自 `reth_cli_runner::CliRunner::try_default_runtime()`（与 tempo 相同）。
10. 测试路径（in-process）：`NodeConfig::test().dev()` + `NodeBuilder::testing_node(task_executor)`（`test-utils` feature，TempDatabase，自动清理）。
11. `PayloadKind::WaitForPending` 语义：等至少一个完整构建。propose 在 leader_timeout 内完成，构建超时风险用 `PayloadKind::Earliest` 兜底（见 §6 风险）。

---

## 3. 详细任务分解

### Phase 0 — 清理与依赖（0.5 天）

**0.1** 删除 `crates/node` 现有三个文件内容（`node.rs`、`consensus.rs` 已腐化），保留目录重建为新集成 crate。

**0.2** workspace `Cargo.toml`：
- `members` 增加 `"crates/node"`。
- `[workspace.dependencies]` 增加（与 tempo 同 rev）：
  ```toml
  reth-ethereum = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384", features = ["node"] }
  reth-chainspec = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-cli-runner = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-node-builder = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-node-core = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-node-ethereum = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-ethereum-engine-primitives = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-engine-primitives = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-payload-primitives = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-payload-builder = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  reth-provider = { git = "https://github.com/paradigmxyz/reth", rev = "1bf2384" }
  alloy-rpc-types-engine = { version = "2.1.1", default-features = false }  # 移到 workspace
  alloy-genesis = "1.6.1"
  ```

**验收**：`cargo metadata` 通过；`cargo check -p allegro-node`（空 crate）通过。

---

### Phase 1 — `allegro-consensus` 扩展点（1 天）

目标：不改任何现有行为默认值，为 reth 模式提供钩子。

**1.1 `BlockInfo` 增加 `timestamp`；verify 时记录区块信息**
- `application.rs`：`BlockInfo { number, hash, view, proposer, timestamp: u64 }`。
- `ValidationResult::Valid` 改为携带元数据：
  ```rust
  pub struct BlockMeta { pub hash: B256, pub number: u64, pub timestamp: u64 }
  pub enum ValidationResult { Valid(BlockMeta), Invalid(String) }
  ```
- `handle_verify`：验证通过后 `block_info.insert(payload_digest, BlockInfo { …, timestamp: meta.timestamp })`。这是 reth 模式的**必要前提**（否则后续 propose 查不到 parent）。
- 同步更新 `StubPayloadBuilder::validate_block`（解码 stub header 返回 meta）与所有测试调用点。

**1.2 `handle_propose` 时间戳规则**
- 改为 `let timestamp = max(now_secs, parent_timestamp + 1)`（parent_timestamp 从 block_info 取；genesis parent 用 chainspec genesis timestamp，DEV 为 0）。

**1.3 `EngineConfig.genesis_hash: B256`（默认 `B256::ZERO`，stub 行为不变）**
- `handle_genesis` 注册 `BlockInfo { number: 0, hash: config.genesis_hash, timestamp: config.genesis_timestamp, … }`。
- `EngineConfig` 同步加 `genesis_timestamp: u64`（默认 0）。

**1.4 finalization sink**
- `EngineConfig.finalized_tx: Option<futures::channel::mpsc::Sender<AllegroDigest>>`。
- `MetricsReporter` 增加 `Option<Sender>` 字段，`Activity::Finalization(cert)` 时 `try_send(cert.proposal.payload)`（满了就 warn + metric，绝不阻塞共识）。

**1.5 `start_simplex_engine` 返回值**
```rust
pub struct StartedEngine {
    pub task: Handle<()>,
    pub block_info: BlockInfoMap,      // finalizer 查询 digest → hash
}
```
- 更新全部调用方：`bin/allegro/src/main.rs`、`crates/consensus/tests/{e2e,multi_node,tx_inclusion}.rs`。

**1.6 `PayloadBuilder::build_payload` 签名不变**（parent_hash/number/timestamp 已够用）。

**验收**：`cargo test -p allegro-consensus` 全绿（stub 路径行为不变）。

---

### Phase 2 — `allegro-reth` attrs 修正（0.5 天）

**2.1** `build_payload_attributes` 修正为 Cancun+ 合法 attrs：
```rust
PayloadAttributes {
    timestamp,
    prev_randao: B256::ZERO,
    suggested_fee_recipient: Address::ZERO,
    withdrawals: Some(vec![]),
    parent_beacon_block_root: Some(B256::ZERO),
    slot_number: None,        // Amsterdam 未激活 ⇒ 必须 None
    target_gas_limit: None,
}
```
- `build_payload_attributes_from_request` 不变。
- 注意 `BuildPayloadRequest.timestamp` 已在 Phase 1.2 保证 > parent_ts。

**2.2** 该 crate 保持**不依赖** reth 主体（consensus 的 dev-dependency 不变重）。

**验收**：`cargo test -p allegro-consensus -p allegro-reth` 全绿；`tx_inclusion` 测试适配新 attrs。

---

### Phase 3 — `allegro-node`：真实集成 crate（2 天）

`crates/node/Cargo.toml` 重写：
```toml
[package]
name = "allegro-node"
# …workspace 继承

[dependencies]
allegro-consensus = { path = "../consensus" }
allegro-reth = { path = "../reth" }
allegro-primitives = { path = "../primitives" }
alloy-primitives.workspace = true
alloy-rlp.workspace = true
alloy-rpc-types-engine.workspace = true
alloy-genesis.workspace = true
eyre.workspace = true
futures.workspace = true
tokio.workspace = true
tracing.workspace = true
reth-ethereum.workspace = true        # features = ["node"]，重导出 NodeBuilder/NodeConfig/EthereumNode
reth-chainspec.workspace = true
reth-cli-runner.workspace = true
reth-node-builder.workspace = true
reth-node-core.workspace = true
reth-node-ethereum.workspace = true
reth-ethereum-engine-primitives.workspace = true
reth-payload-primitives.workspace = true

[dev-dependencies]
reth-ethereum = { workspace = true, features = ["test-utils"] }
tempfile.workspace = true
```

**3.1 `chainspec.rs`**
```rust
/// v1：直接用 reth_chainspec::DEV（chain id 1337，20 个预注资账户）。
/// v2（可选）：基于 DEV_HARDFORKS + 自定义 genesis（xtask 生成）构造 ChainSpec::builder()。
pub fn dev_chainspec() -> Arc<ChainSpec> { DEV.clone() }
```

**3.2 `launch.rs` — 节点启动**
```rust
pub struct RethNodeConfig {
    pub datadir: PathBuf,
    pub http_port: u16,        // 8545+i
    pub authrpc_port: u16,     // 8551+i
    pub p2p_port: u16,         // 30303+i
    pub chain: Arc<ChainSpec>,
}

pub struct LaunchedRethNode {
    pub engine_handle: ConsensusEngineHandle<EthPayloadTypes>,
    pub payload_builder_handle: PayloadBuilderHandle<EthPayloadTypes>,
    pub genesis_hash: B256,
    pub genesis_timestamp: u64,
    pub node: TempoLikeFullNode,   // reth FullNode<EthereumNode> 别名
    pub exit: impl Future<Output = eyre::Result<()>>,
}

pub async fn launch(cfg: RethNodeConfig, task_executor: TaskExecutor) -> eyre::Result<LaunchedRethNode> {
    let node_config = NodeConfig::new(cfg.chain.clone())
        .with_datadir_args(DatadirArgs { datadir: cfg.datadir.into(), ..Default::default() })
        .with_rpc(RpcServerArgs::default().with_http().with_http_port(cfg.http_port)
                  .with_authrpc_port(cfg.authrpc_port))
        .with_network(NetworkArgs::default().with_port(cfg.p2p_port).with_discovery(None));
        // 关键：不调用 .dev()（§2.8）
    let NodeHandle { node, node_exit_future } = NodeBuilder::new(node_config)
        .with_launch_context(task_executor)
        .node(EthereumNode)
        .launch()
        .await?;
    let genesis_hash = node.chain_spec().genesis_header().hash();
    // …填 LaunchedRethNode
}
```

**3.3 `builder.rs` — payload builder 闭包（核心，~120 行）**
```rust
pub struct ForkchoiceTracker {           // finalizer 与 build 闭包共享
    finalized: parking_lot::RwLock<(B256, u64)>,   // (hash, number)，初始 (genesis_hash, 0)
}

pub fn create_engine_payload_builder(
    engine: ConsensusEngineHandle<EthPayloadTypes>,
    payloads: PayloadBuilderHandle<EthPayloadTypes>,
    tracker: Arc<ForkchoiceTracker>,
) -> Arc<dyn PayloadBuilder> { … }
```

build 闭包逻辑：
```rust
|req: BuildPayloadRequest| {
    let (fcs, attrs) = build_payload_attributes_from_request(&req);
    let fcs = ForkchoiceState { head_block_hash: req.parent_hash,
                                safe_block_hash: tracker.finalized().0,
                                finalized_block_hash: tracker.finalized().0 };
    // 1. FCU → payload_id
    let resp = engine.fork_choice_updated(fcs, Some(attrs)).await.map_err(err)?;
    let payload_id = resp.payload_id.ok_or("engine returned no payload id")?;
    // 2. resolve（WaitForPending；超时兜底 Earliest，见 §6）
    let payload = payloads.resolve_kind(payload_id, PayloadKind::WaitForPending)
        .await.ok_or("payload job missing")?.map_err(err)?;
    let sealed = payload.block().clone();                    // SealedBlock<Block>
    // 3. 自导入（以太坊 CL 标准行为；保证 finalize 时 FCU 目标在树中）
    let data = EthPayloadTypes::block_to_payload(sealed.clone(), None);
    match engine.new_payload(data).await { … Valid/Accepted ⇒ ok, 其余 ⇒ Err … }
    // 4. RLP 编码标准 Block
    let block = sealed.into_block();
    let bytes = alloy_rlp::encode(&block);
    Ok(BuiltPayload { block_bytes: bytes, block_hash: block.header.hash_slow(), block_number: block.header.number })
}
```

validate 闭包逻辑：
```rust
|req: ValidateBlockRequest| {
    let block: alloy_consensus::Block<TxEnvelope, alloy_consensus::Header> =
        alloy_rlp::Decodable::decode(&mut &req.block_bytes[..]).map_err(err)?;
    let hash = block.header.hash_slow();
    let sealed = SealedBlock::seal_slow(block);   // 或用 SealedBlock::new_unchecked(header.seal(hash), body)
    let data = EthPayloadTypes::block_to_payload(sealed, None);
    match engine.new_payload(data).await {
        Ok(status) => match status.status {
            PayloadStatusEnum::Valid => Ok(ValidationResult::Valid(BlockMeta {
                hash, number: block.header.number, timestamp: block.header.timestamp })),
            PayloadStatusEnum::Invalid { validation_error } =>
                Ok(ValidationResult::Invalid(validation_error)),
            PayloadStatusEnum::Syncing | PayloadStatusEnum::Accepted =>
                Err("parent unknown to execution layer (syncing)".into()),
        },
        Err(e) => Err(format!("engine new_payload: {e}")),
    }
}
```

**3.4 `finalizer.rs` — finalization → FCU（~60 行）**
```rust
pub fn spawn_finalizer<R: commonware_runtime::Spawner>(
    ctx: R,
    mut rx: mpsc::Receiver<AllegroDigest>,
    block_info: BlockInfoMap,
    engine: ConsensusEngineHandle<EthPayloadTypes>,
    tracker: Arc<ForkchoiceTracker>,
)
// 每条 finalized digest：
//   let hash = block_info[digest].hash（查不到 ⇒ warn + metric，跳过）
//   FCU(head=hash, safe=hash, finalized=hash)；成功 ⇒ tracker.update(hash, number)
//   返回 SYNCING ⇒ warn（节点漏验了该块，EL 缺块；见 §6 已知限制）
```

**验收（crate 内集成测试，`crates/node/tests/engine_roundtrip.rs`）**：
in-process 启动单 reth 节点（§2.10 测试路径）→ 注入 1 笔 tx → 走 build 闭包产出 block 1 → 走 validate 闭包 ⇒ Valid → 手动 FCU(finalized=block1) → 断言 `provider` 的 canonical head = block 1 且含该 tx。

---

### Phase 4 — `bin/allegro` 接线（1 天）

**4.1 CLI（clap）**
```rust
#[arg(long = "execution", default_value = "reth", env = "ALLEGRO_EXECUTION")]
pub execution: ExecutionMode,          // reth | stub
// reth 组：
#[arg(long = "datadir", env = "ALLEGRO_DATADIR")] pub datadir: Option<PathBuf>,
#[arg(long = "rpc-port", default_value = "8545", env = "ALLEGRO_RPC_PORT")] pub rpc_port: u16,
#[arg(long = "authrpc-port", default_value = "8551", env = "ALLEGRO_AUTHRPC_PORT")] pub authrpc_port: u16,
#[arg(long = "reth-p2p-port", default_value = "30303", env = "ALLEGRO_RETH_P2P_PORT")] pub reth_p2p_port: u16,
```
- 默认 `reth`（本次集成的目的）；`stub` 保留给 deterministic 测试与轻量联调。
- `--node N` 用于端口偏移由调用方/devnet 脚本处理，CLI 直接吃绝对端口（保持简单）。

**4.2 启动流程（`main.rs` 重构）**
```rust
fn main() -> eyre::Result<()> {
    let cli = Cli::parse(); init_tracing(&cli);
    match cli.execution {
        ExecutionMode::Stub => run_stub(cli),            // 现有代码原样抽函数
        ExecutionMode::Reth => run_reth(cli),
    }
}

fn run_reth(cli: Cli) -> eyre::Result<()> {
    let runner = reth_cli_runner::CliRunner::try_default_runtime()?;   // reth tokio runtime
    let (node_tx, node_rx) = std::sync::mpsc::channel();

    // 共识线程（commonware runtime）
    let consensus = std::thread::spawn(move || {
        let launched = node_rx.recv().expect("reth node handle");       // LaunchedRethNode 的 handle 部分（Send）
        let rt_cfg = commonware_runtime::tokio::Config::new()
            .with_storage_directory(consensus_dir)
            .with_catch_panics(true);
        commonware_runtime::tokio::Runner::new(rt_cfg).start(|ctx| async move {
            // 1. p2p 网络（现有代码）
            // 2. tracker + finalized_tx/rx + spawn_finalizer
            // 3. payload_builder = create_engine_payload_builder(engine, payloads, tracker)
            // 4. start_simplex_engine(EngineConfig {
            //        genesis_hash: launched.genesis_hash,
            //        genesis_timestamp: launched.genesis_timestamp,
            //        finalized_tx: Some(tx),
            //        payload_builder: Some(payload_builder), … }) → StartedEngine
            // 5. 常驻 loop
        })
    });

    // 主线程：启动 reth，发 handle 给共识线程
    runner.block_on(async move {
        let launched = allegro_node::launch(reth_cfg, task_executor).await?;
        node_tx.send(launched.handles()).unwrap();
        tokio::select! {
            _ = launched.exit => info!("reth exited"),
            _ = tokio::signal::ctrl_c() => info!("ctrl-c"),
        }
        Ok(())
    })?;
    consensus.join().expect("consensus thread panic")?;
    Ok(())
}
```
- 参考 tempo `bin/tempo/src/lib.rs` 的双 runtime 模式；`ConsensusEngineHandle`/`PayloadBuilderHandle` 均可 `Send`，跨 runtime await 安全（§2.1）。
- `EngineConfig.partition` 已按 node 隔离；consensus storage 目录放到 `datadir/consensus`。

**4.3 优雅关闭**
- ctrl-c → reth runtime graceful shutdown → 共识线程收到信号退出（v1：join 超时直接 abort 进程即可，记 TODO）。

**验收**：`allegro --node 0 --listen 127.0.0.1:13000` 单节点起来后：
- `cast block-number --rpc-url localhost:8545` 持续增长；
- `cast send --private-key <anvil-0> --value 1ether <addr> --rpc-url localhost:8545` 后交易被打包进块。

---

### Phase 5 — 测试（1.5 天）

| 测试 | 位置 | 内容 |
|---|---|---|
| 单元 | `allegro-node/src/chainspec.rs` | DEV spec genesis hash 稳定、Osaka 激活、Amsterdam 未激活 |
| 单元 | `allegro-reth` | attrs 满足 §2.5/2.6 校验（withdrawals/root Some、slot_number None、ts > parent） |
| 集成 | `crates/node/tests/engine_roundtrip.rs` | §3 验收：build→validate→finalize→head 推进 + tx 入块 |
| 集成 | `crates/node/tests/consensus_reth.rs` | **3 节点 in-process**：每节点起一个 reth（TempDatabase）+ commonware deterministic? ❌ deterministic runtime 不能跑 reth（需要真实 tokio）⇒ 用 `commonware_runtime::tokio::Runner` + simulated/lookup p2p，跑若干轮，断言三节点 canonical head 一致且 > 0 |
| 进程级 | `bin/allegro/tests/process_e2e.rs` | 新增 `test_two_node_reth_devnet`：spawn 2 个 `--execution reth` 进程（tempdir datadir），等产出块，curl 两个 RPC 比较 block number；保留现有 stub 用例（显式 `--execution stub`） |
| 回归 | `crates/consensus/tests/*` | Phase 1 接口变更适配后全绿（deterministic 不变） |

---

### Phase 6 — devnet 脚本与文档（0.5 天）

- `run_devnet.sh` / `run_devnet.py`：为每节点分配 `--datadir $DATA_DIR/node$i`、`--rpc-port 8545+i`、`--authrpc-port 8551+i`、`--reth-p2p-port 30303+i`；启动后 `cast block-number` 探活；可选 `cast send` 冒烟。
- `xtask`（可选 P1）：`genesis` 子命令输出自定义 chainspec genesis.json（含验证人 funding）。
- `README.md`：更新 quickstart（reth 模式为默认）。
- `docs/reth-integration-analysis.md`：末尾追加 "Implementation Status" 小节，链接本计划并标记完成项。

---

## 4. 时序与依赖

```
Phase 0 (0.5d) ──┬─► Phase 1 (1d) ──► Phase 2 (0.5d) ──► Phase 3 (2d) ──► Phase 4 (1d) ──► Phase 5 (1.5d) ──► Phase 6 (0.5d)
                 │   （P1/P2 可并行；P3 依赖 P1 的 BlockMeta/StartedEngine 与 P2 的 attrs）
                 └── 总计约 7 天（含联调缓冲）
```

---

## 5. 验收标准（Definition of Done）

1. `cargo check --workspace` 与 `cargo test --workspace` 全绿。
2. 单节点：`allegro --execution reth` 启动后 RPC `eth_blockNumber` 持续增长（共识驱动出块）。
3. 交易闭环：`eth_sendRawTransaction`（anvil 账户）→ 若干块后 `eth_getTransactionReceipt` 成功。
4. 4 节点 devnet（`run_devnet.sh 4`）：所有节点 `eth_blockNumber` 一致推进；任一节点收到 tx 后最终入块（round-robin 保证 ≤ N 个 view）。
5. `eth_getBlockByNumber("finalized")` 返回块随 finalization 证书推进。
6. stub 模式与既有 deterministic 测试不受影响。

---

## 6. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| payload 构建慢于 leader_timeout | propose 失败、view 空转 | build 闭包先 `WaitForPending` 并带超时（如 800ms），超时回落 `Earliest`（可能拿空块但合法）；devnet 池小，实测后调参 |
| 节点漏验某块（网络慢）→ EL 无该块 → finalize FCU 返回 SYNCING | 该节点 EL 停住，共识继续 | v1 记 warn + metric；v2 做 backfill（按高度从 peer 拉块补 new_payload），对齐 tempo 的 syncer |
| 重启后 BlockInfoMap 丢失 | propose 查 parent 失败 → build 报错 | 已知限制写入文档；v2 持久化 block_info 或从 marshal 回放重建 |
| 秒级时间戳限制出块 ≤ 1 块/秒 | devnet 出块间隔下限 1s | 接受（tempo 用自定义 attrs 带 millis；Allegro 用标准 attrs，保持简单）。leader_timeout 默认 2s 已兼容 |
| 多节点 reth devp2p 不互通 → tx 只在接收节点池中 | tx 需等接收节点当上 leader 才入块 | v1 接受（round-robin 有界）；P1 增强：静态 trusted peers 互联（记录各节点 enode，`network.add_peer_kind` 注入，参考 tempo bootnodes 流程） |
| reth 编译耗时（首次 ~10 分钟级） | 开发体验 | 仅 `allegro-node`/bin 依赖 reth；consensus/primitives 保持轻量，日常测试不受影响 |

---

## 7. 明确不做（Out of Scope）

- 自定义 `NodeTypes`/`Primitives`/`AddOns`/EVM/Pool（分析文档结论：全部用 reth 默认）。
- 区块同步/回填协议（tempo 的 follow/syncer 栈）。
- 交易池跨节点互通（列为 P1 增强）。
- 毫秒级时间戳（需自定义 payload attributes，违背"标准 EthPayloadTypes"决策）。
- 生产级 chainspec 管理、checkpoint/sync、监控告警。
- `AllegroHeader` 共识上下文上链（reth 模式下共识上下文由 actor 的 BlockInfoMap 承载，不进 header；header 保持标准以太坊格式）。
