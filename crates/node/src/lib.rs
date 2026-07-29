//! Allegro-Reth node integration.
//!
//! This crate provides the production payload builder, chainspec helpers, and
//! finalization forwarding needed to embed a reth execution node inside
//! Allegro's commonware consensus engine.
//!
//! See [`docs/reth-integration-plan.md`] for the full architecture and
//! implementation status.

pub mod allegro_consensus;
pub mod builder;
pub mod chainspec;
pub mod finalizer;
pub mod launch;
