//! Error types for the consensus crate.

use thiserror::Error;

/// Consensus-level errors.
#[derive(Debug, Error)]
pub enum ConsensusError {
    /// Invalid validator configuration.
    #[error("invalid validator configuration: {0}")]
    InvalidValidatorConfig(String),

    /// Payload builder error.
    #[error("payload builder error: {0}")]
    PayloadBuilder(String),

    /// Block not found for verification.
    #[error("block not found: {0}")]
    BlockNotFound(String),

    /// Proposer is not a known validator.
    #[error("unknown proposer: {0}")]
    UnknownProposer(String),

    /// Network error.
    #[error("network error: {0}")]
    Network(String),

    /// Storage/persistence error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Clock/timestamp error.
    #[error("timestamp error: {0}")]
    Timestamp(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Internal inconsistency.
    #[error("internal error: {0}")]
    Internal(String),

    /// Block validation failed.
    #[error("block validation failed: {0}")]
    BlockValidation(String),
}
