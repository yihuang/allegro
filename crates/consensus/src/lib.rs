//! Allegro consensus — Commonware integration for Reth.
//!
//! This crate provides the consensus layer that connects
//! Commonware's threshold simplex consensus with a Reth execution layer.
//!
//! ## Architecture
//!
//! The consensus layer follows the actor pattern established by Tempo:
//!
//! - `Application` — implements [`commonware_consensus::Automaton`] via a
//!   mailbox/actor split, bridging consensus callbacks to the execution layer.
//! - `Executor` — forwards finalized blocks and fork-choice updates to Reth.
//! - `Block` — wraps a sealed execution block with the codec traits
//!   ([`Digestible`], [`Committable`], [`EncodeSize`], [`Read`], [`Write`])
//!   needed by commonware's marshal and p2p layers.
//! - `ValidatorSet` — holds the active Ed25519 identities.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub(crate) mod block;
mod config;
pub(crate) mod executor;
pub(crate) mod validators;

// TODO: implement application actor with Automaton trait
// pub(crate) mod application;

// TODO: implement consensus engine assembly
// pub(crate) mod engine;

pub use validators::{ValidatorEntry, ValidatorSet};

/// Placeholder for the consensus engine entry point.
///
/// In the full implementation this initializes the p2p network,
/// the commonware simplex consensus engine, the marshal actor,
/// the executor actor, and runs them all in a `tokio::select!`.
pub async fn run_consensus_stack(
    _signing_key: commonware_cryptography::ed25519::PrivateKey,
    _validators: ValidatorSet,
    _listen_address: std::net::SocketAddr,
) -> eyre::Result<()> {
    // TODO: wire up commonware-consensus simplex engine
    Err(eyre::eyre!("consensus engine not yet implemented"))
}
