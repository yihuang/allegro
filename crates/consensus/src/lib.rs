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
//! - `executor` — Payload builder abstraction over reth's engine API
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
pub use engine::{start_simplex_engine, EngineConfig, StartedEngine};
pub use error::ConsensusError;
pub use executor::{
    build_payload_attributes, create_reth_payload_builder, millis_from_secs, BlockMeta,
    BuildPayloadRequest, BuiltPayload, EngineApiPayloadBuilder, PayloadBuilder,
    ValidateBlockRequest, ValidationResult,
};
pub use metrics::ConsensusMetrics;
pub use validators::{ValidatorEntry, ValidatorSet};
