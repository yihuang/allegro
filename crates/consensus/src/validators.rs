//! Minimal validator management for Allegro.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use commonware_cryptography::ed25519::PublicKey;
use commonware_p2p::{Address, Ingress};
use parking_lot::RwLock;
use tracing::info;

/// A hardcoded validator entry.
#[derive(Clone, Debug)]
pub struct ValidatorEntry {
    pub public_key: PublicKey,
    pub ingress: SocketAddr,
    pub egress: IpAddr,
}

/// Set of validators with their P2P addresses.
#[derive(Clone)]
pub struct ValidatorSet {
    inner: Arc<RwLock<HashMap<PublicKey, Address>>>,
}

impl ValidatorSet {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn from_entries(entries: &[ValidatorEntry]) -> Self {
        let mut map = HashMap::new();
        for entry in entries {
            let addr = Address::Asymmetric {
                ingress: Ingress::Socket(entry.ingress),
                egress: SocketAddr::from((entry.egress, 0)),
            };
            info!(public_key = %entry.public_key, "adding validator");
            map.insert(entry.public_key.clone(), addr);
        }
        Self { inner: Arc::new(RwLock::new(map)) }
    }

    /// Return all validator public keys.
    pub fn keys(&self) -> Vec<PublicKey> {
        self.inner.read().keys().cloned().collect()
    }

    /// Look up a validator's address by public key.
    pub fn lookup(&self, key: &PublicKey) -> Option<Address> {
        self.inner.read().get(key).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for ValidatorSet {
    fn default() -> Self { Self::new() }
}
