//! Custom beacon consensus and engine validator for Allegro.
//!
//! - [`AllegroBeaconConsensus`]: wraps [`reth_ethereum_consensus::EthBeaconConsensus`],
//!   relaxes the timestamp check in `validate_header_against_parent` to `>=`.
//! - [`AllegroEngineValidator`]: wraps [`reth_ethereum::node::EthereumEngineValidator`],
//!   relaxes the payload-attributes timestamp check to `>=`.
//!
//! With sub-second block times from simplex consensus, consecutive blocks
//! may share the same `timestamp` (seconds) field — monotonicity is ensured
//! by the millisecond-precision `timestamp_millis` field in
//! [`AllegroHeader`](allegro_primitives::AllegroHeader).

use std::sync::Arc;

use alloy_consensus::BlockHeader as _;
use alloy_primitives::B256;
use alloy_rpc_types_engine::ExecutionData;
use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardforks};
use reth_consensus::{
    Consensus, ConsensusError, FullConsensus, HeaderValidator, ReceiptRootBloom, TransactionRoot,
};
use reth_consensus_common::validation::{
    validate_against_parent_4844, validate_against_parent_eip1559_base_fee,
    validate_against_parent_gas_limit, validate_against_parent_hash_number,
};
use reth_engine_primitives::EngineApiValidator;
use reth_engine_primitives::PayloadValidator as PayloadValidatorTrait;
pub use reth_ethereum_consensus::validate_block_post_execution;
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_execution_types::BlockExecutionResult;
use reth_node_api::{AddOnsContext, EngineTypes, FullNodeComponents, NodeTypes, PayloadTypes};
use reth_node_builder::components::ConsensusBuilder;
use reth_node_builder::rpc::PayloadValidatorBuilder;
use reth_node_builder::{BuilderContext, FullNodeTypes};
use reth_payload_primitives::{EngineApiMessageVersion, InvalidPayloadAttributesError};
use reth_payload_primitives::{
    EngineObjectValidationError, NewPayloadError, PayloadAttributes, PayloadOrAttributes,
};
use reth_primitives_traits::{
    Block as BlockTrait, NodePrimitives, RecoveredBlock, SealedBlock, SealedHeader,
};

type EthBlock = reth_ethereum_primitives::Block;

// ═══════════════════════════════════════════════════════════
//  CONSENSUS
// ═══════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct AllegroBeaconConsensus {
    inner: EthBeaconConsensus<ChainSpec>,
}

impl AllegroBeaconConsensus {
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            inner: EthBeaconConsensus::new(chain_spec),
        }
    }
    pub fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.inner.chain_spec()
    }
}

impl HeaderValidator<alloy_consensus::Header> for AllegroBeaconConsensus {
    fn validate_header(
        &self,
        header: &SealedHeader<alloy_consensus::Header>,
    ) -> Result<(), ConsensusError> {
        self.inner.validate_header(header)
    }
    fn validate_header_against_parent(
        &self,
        header: &SealedHeader<alloy_consensus::Header>,
        parent: &SealedHeader<alloy_consensus::Header>,
    ) -> Result<(), ConsensusError> {
        validate_against_parent_hash_number(header.header(), parent)?;
        if header.header().timestamp() < parent.header().timestamp() {
            return Err(ConsensusError::TimestampIsInPast {
                parent_timestamp: parent.header().timestamp(),
                timestamp: header.header().timestamp(),
            });
        }
        validate_against_parent_gas_limit(header, parent, self.chain_spec())?;
        validate_against_parent_eip1559_base_fee(
            header.header(),
            parent.header(),
            self.chain_spec(),
        )?;
        if let Some(blob_params) = self
            .chain_spec()
            .blob_params_at_timestamp(header.header().timestamp())
        {
            validate_against_parent_4844(header.header(), parent.header(), blob_params)?;
        }
        Ok(())
    }
}

impl Consensus<EthBlock> for AllegroBeaconConsensus {
    fn validate_body_against_header(
        &self,
        body: &<EthBlock as BlockTrait>::Body,
        header: &SealedHeader<<EthBlock as BlockTrait>::Header>,
    ) -> Result<(), ConsensusError> {
        <EthBeaconConsensus<ChainSpec> as Consensus<EthBlock>>::validate_body_against_header(
            &self.inner,
            body,
            header,
        )
    }
    fn validate_block_pre_execution(
        &self,
        block: &SealedBlock<EthBlock>,
    ) -> Result<(), ConsensusError> {
        self.inner.validate_block_pre_execution(block)
    }
    fn validate_block_pre_execution_with_tx_root(
        &self,
        block: &SealedBlock<EthBlock>,
        transaction_root: Option<TransactionRoot>,
    ) -> Result<(), ConsensusError> {
        self.inner
            .validate_block_pre_execution_with_tx_root(block, transaction_root)
    }
}

impl FullConsensus<reth_ethereum_primitives::EthPrimitives> for AllegroBeaconConsensus {
    fn validate_block_post_execution(
        &self,
        block: &RecoveredBlock<<reth_ethereum_primitives::EthPrimitives as NodePrimitives>::Block>,
        result: &BlockExecutionResult<
            <reth_ethereum_primitives::EthPrimitives as NodePrimitives>::Receipt,
        >,
        receipt_root_bloom: Option<ReceiptRootBloom>,
        block_access_list_hash: Option<B256>,
    ) -> Result<(), ConsensusError> {
        validate_block_post_execution(
            block,
            self.chain_spec(),
            result,
            receipt_root_bloom,
            block_access_list_hash,
        )
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AllegroConsensusBuilder;

impl<Node> ConsensusBuilder<Node> for AllegroConsensusBuilder
where
    Node: FullNodeTypes<Types = reth_ethereum::node::EthereumNode>,
{
    type Consensus = Arc<AllegroBeaconConsensus>;
    async fn build_consensus(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::Consensus> {
        Ok(Arc::new(AllegroBeaconConsensus::new(ctx.chain_spec())))
    }
}

// ═══════════════════════════════════════════════════════════
//  ENGINE VALIDATOR
// ═══════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct AllegroEngineValidator<C = ChainSpec> {
    inner: reth_ethereum::node::EthereumEngineValidator<C>,
}

impl<C> AllegroEngineValidator<C> {
    pub fn new(chain_spec: Arc<C>) -> Self {
        Self {
            inner: reth_ethereum::node::EthereumEngineValidator::new(chain_spec),
        }
    }
}

impl<C, Types> PayloadValidatorTrait<Types> for AllegroEngineValidator<C>
where
    C: EthChainSpec + EthereumHardforks + 'static,
    Types: PayloadTypes<ExecutionData = ExecutionData>,
{
    type Block = reth_ethereum_primitives::Block;

    fn convert_payload_to_block(
        &self,
        payload: ExecutionData,
    ) -> Result<SealedBlock<Self::Block>, NewPayloadError> {
        <reth_ethereum::node::EthereumEngineValidator<C> as PayloadValidatorTrait<Types>>::convert_payload_to_block(
            &self.inner, payload,
        )
    }

    fn validate_payload_attributes_against_header(
        &self,
        attr: &Types::PayloadAttributes,
        header: &<Self::Block as BlockTrait>::Header,
    ) -> Result<(), InvalidPayloadAttributesError> {
        if attr.timestamp() < header.timestamp() {
            return Err(InvalidPayloadAttributesError::InvalidTimestamp);
        }
        Ok(())
    }
}

impl<C, Types> EngineApiValidator<Types> for AllegroEngineValidator<C>
where
    C: EthChainSpec + EthereumHardforks + 'static,
    Types: PayloadTypes<
        PayloadAttributes = alloy_rpc_types_engine::PayloadAttributes,
        ExecutionData = ExecutionData,
    >,
{
    fn validate_version_specific_fields(
        &self,
        version: EngineApiMessageVersion,
        payload_or_attrs: PayloadOrAttributes<'_, Types::ExecutionData, Types::PayloadAttributes>,
    ) -> Result<(), EngineObjectValidationError> {
        <reth_ethereum::node::EthereumEngineValidator<C> as EngineApiValidator<Types>>::validate_version_specific_fields(
            &self.inner, version, payload_or_attrs,
        )
    }
    fn ensure_well_formed_attributes(
        &self,
        version: EngineApiMessageVersion,
        attributes: &Types::PayloadAttributes,
    ) -> Result<(), EngineObjectValidationError> {
        <reth_ethereum::node::EthereumEngineValidator<C> as EngineApiValidator<Types>>::ensure_well_formed_attributes(
            &self.inner, version, attributes,
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct AllegroEngineValidatorBuilder;

impl<Node> PayloadValidatorBuilder<Node> for AllegroEngineValidatorBuilder
where
    Node: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec: EthereumHardforks + Clone + 'static,
            Payload: EngineTypes<ExecutionData = ExecutionData>
                         + PayloadTypes<PayloadAttributes = alloy_rpc_types_engine::PayloadAttributes>,
            Primitives = reth_ethereum_primitives::EthPrimitives,
        >,
    >,
{
    type Validator = AllegroEngineValidator<<Node::Types as NodeTypes>::ChainSpec>;

    async fn build(self, ctx: &AddOnsContext<'_, Node>) -> eyre::Result<Self::Validator> {
        Ok(AllegroEngineValidator::new(ctx.config.chain.clone()))
    }
}
