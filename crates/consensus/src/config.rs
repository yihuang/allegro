//! Consensus configuration constants.

/// The application namespace.
pub(crate) const NAMESPACE: &[u8] = b"ALLEGRO";

/// Channel identifiers for commonware p2p.
pub(crate) const VOTES_CHANNEL_IDENT: commonware_p2p::Channel = 0;
pub(crate) const CERTIFICATES_CHANNEL_IDENT: commonware_p2p::Channel = 1;
pub(crate) const RESOLVER_CHANNEL_IDENT: commonware_p2p::Channel = 2;
pub(crate) const BROADCASTER_CHANNEL_IDENT: commonware_p2p::Channel = 3;
pub(crate) const MARSHAL_CHANNEL_IDENT: commonware_p2p::Channel = 4;
