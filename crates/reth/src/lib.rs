//! Reth integration for Allegro consensus.
//!
//! Provides helper functions to build and validate blocks via reth's engine API.
//! The actual engine API calls are made by closures provided by the binary,
//! keeping the consensus crate decoupled from reth's type system.
//!
//! See [`payload::create_reth_payload_builder`] for the main entry point.

mod payload;
pub use payload::{
    build_payload_attributes, build_payload_attributes_from_request,
    create_reth_payload_builder,
};
