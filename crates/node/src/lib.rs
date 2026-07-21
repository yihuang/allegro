//! Allegro node types — WORK IN PROGRESS.
//!
//! This crate provides the Reth node integration for Allegro.
//! The full implementation requires:
//!
//! - `AllegroNode` implementing `NodeTypes` + `Node`
//! - `AllegroConsensus` implementing `HeaderValidator` + `Consensus` + `FullConsensus`
//! - `AllegroConsensusBuilder` implementing `ConsensusBuilder`
//! - `AllegroEngineValidator` implementing `PayloadValidator`
//! - `AllegroAddOns` for RPC modules
//!
//! See the `allegro-primitives` and `allegro-consensus` crates for the
//! foundational types and commonware integration.

#![allow(dead_code)]

/// Placeholder for the Allegro node type.
pub struct AllegroNode;
