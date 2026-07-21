//! Application actor — implements [`commonware_consensus::Automaton`].
//!
//! This is the bridge between the consensus layer and the execution layer.

use std::sync::Arc;

use alloy_consensus::{BlockHeader, Sealable};
use alloy_primitives::B256;
use commonware_codec::{Encode, EncodeSize};
use commonware_consensus::{
    Automaton, Heightable,
    types::{Epoch, Height, View},
};
use commonware_cryptography::{
    Digestible,
    ed25519::{PrivateKey, PublicKey},
};
use commonware_runtime::{Clock, Metrics, Pacer, Spawner};
use eyre::{OptionExt, WrapErr};
use tracing::{debug, info, instrument, warn};

use allegro_primitives::{AllegroConsensusContext, AllegroHeader, Digest};

use crate::{
    block::Block,
    executor::{self, Command},
    validators::ValidatorSet,
};

/// Configuration for the application actor.
pub(crate) struct Config {
    pub(crate) public_key: PublicKey,
    pub(crate) signing_key: PrivateKey,
    pub(crate) mailbox_size: usize,
    pub(crate) executor_mailbox: executor::Mailbox,
    pub(crate) validators: ValidatorSet,
}

/// The application actor implementing the `Automaton` trait.
pub(crate) struct Application<TContext> {
    context: TContext,
    config: Config,
}

impl<TContext> Application<TContext>
where
    TContext: Clock + Metrics + Pacer + Spawner,
{
    pub(crate) fn new(context: TContext, config: Config) -> Self {
        Self { context, config }
    }
}

/// The payload type returned by `propose` — a raw sealed block.
pub(crate) struct AllegroProposal {
    pub(crate) block: alloy_consensus::SealedBlock<allegro_primitives::Block>,
}

// ── Automaton implementation ──
//
// Commonware's Automaton trait defines the three consensus callbacks:
//
//   propose(epoch, view)  →  called when this node is the leader
//   verify(proposal)      →  called when a proposal arrives
//   commit(proposal)      →  called when a proposal and its ancestors finalize

impl<TContext> Automaton for Application<TContext>
where
    TContext: Clock + Metrics + Pacer + Spawner + 'static,
{
    type Digest = Digest;
    type Proposal = Block;
    type Context = ();

    fn propose(
        &mut self,
        _context: &mut Self::Context,
        epoch: Epoch,
        view: View,
    ) -> Result<Self::Proposal, <Self as Automaton>::Error> {
        info!(%epoch, %view, "propose called — building block");

        // In a full integration, this would:
        // 1. Call reth's payload builder to construct a block
        // 2. Attach consensus context (epoch, view, proposer)
        // 3. Seal and return
        //
        // For the minimal demo, we signal intent and return a stub.
        // The actual payload building happens via the reth engine API.

        Err(<Self as Automaton>::Error::NotProposing)
    }

    fn verify(
        &mut self,
        _context: &mut Self::Context,
        proposal: &Self::Proposal,
    ) -> Result<(), <Self as Automaton>::Error> {
        let header = proposal.inner().header();
        let hash = proposal.inner().hash();

        debug!(
            number = header.number(),
            hash = %hash,
            "verify called — validating block",
        );

        // Validate the consensus context
        let ctx = header
            .consensus_context
            .as_ref()
            .ok_or(<Self as Automaton>::Error::InvalidProposal(
                "block missing consensus context".into(),
            ))?;

        // Verify the proposer is a known validator
        if self.config.validators.lookup(&ctx.proposer).is_none() {
            warn!(
                proposer = %ctx.proposer,
                "proposer is not in the active validator set"
            );
            return Err(<Self as Automaton>::Error::InvalidProposal(
                "unknown proposer".into(),
            ));
        }

        // In a full integration, this would execute the block and verify
        // the state root matches.

        Ok(())
    }

    fn commit(
        &mut self,
        _context: &mut Self::Context,
        proposal: &Self::Proposal,
    ) -> Result<(), <Self as Automaton>::Error> {
        let header = proposal.inner().header();
        let hash = proposal.inner().hash();
        let height = header.number();

        info!(
            %height,
            hash = %hash,
            "commit called — block finalized",
        );

        // Forward to executor for forkchoice update
        self.config
            .executor_mailbox
            .send(Command::Finalized {
                height: Height::new(height),
                block: proposal.clone(),
            })
            .map_err(|e| {
                <Self as Automaton>::Error::Internal(e.to_string())
            })?;

        Ok(())
    }
}
