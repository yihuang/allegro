//! Allegro consensus — Commonware integration for Reth.
//!
//! Adapted from Tempo. Provides the consensus layer that connects
//! Commonware's threshold simplex consensus with a Reth execution layer.
//!
//! ## Architecture
//!
//! - `application` — [`Automaton`] implementation via Mailbox/Actor pattern
//! - `block` — Block wrapper with [`Digestible`], [`Committable`], commonware codec traits
//! - `config` — Tunable consensus parameters with production defaults
//! - `engine` — Simplex engine bootstrap with p2p, block relay, and persistence
//! - `error` — Typed errors for the consensus crate
//! - `executor` — Payload builder abstraction (stub, engine API)
//! - `metrics` — Consensus metrics counters
//! - `validators` — Ed25519 validator set management

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod application;
pub(crate) mod block;
pub mod config;
pub(crate) mod engine;
pub mod error;
pub mod executor;
pub mod loopback;
pub mod metrics;
pub(crate) mod validators;

pub use block::Block;
pub use engine::{EngineConfig, StartedEngine, start_simplex_engine};
pub use error::ConsensusError;
pub use executor::{
    build_empty_block_internal, BlockMeta, BuildPayloadRequest, BuiltPayload,
    EngineApiPayloadBuilder, PayloadBuilder, StubPayloadBuilder, ValidateBlockRequest,
    ValidationResult,
};
pub use metrics::ConsensusMetrics;
pub use validators::{ValidatorEntry, ValidatorSet};
