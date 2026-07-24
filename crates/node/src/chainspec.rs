//! Chainspec helpers for the Allegro devnet.

use std::sync::Arc;

use reth_chainspec::ChainSpec;

/// Return the DEV chainspec, which activates all hardforks through Osaka at genesis
/// and includes 20 prefunded accounts derived from the "test test test ... junk" mnemonic.
///
/// This is the simplest chainspec for local devnet usage. Each node shares the same
/// spec — the genesis hash is identical across the network.
pub fn dev_chainspec() -> Arc<ChainSpec> {
    reth_chainspec::DEV.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_hash_is_stable() {
        let spec = dev_chainspec();
        let hash = spec.genesis_hash();
        // DEV spec genesis hash from reth_chainspec (hardcoded reference).
        // DEV spec genesis hash (deterministic).
        assert_eq!(
            hash,
            alloy_primitives::b256!(
                "683713729fcb72be6f3d8b88c8cda3e10569d73b9640d3bf6f5184d94bd97616"
            )
        );
    }

    #[test]
    fn amsterdam_not_activated_at_genesis() {
        use reth_chainspec::EthereumHardforks;
        let spec = dev_chainspec();
        // DEV hardforks only go up to Osaka at ts=0; Amsterdam is NOT activated.
        assert!(!spec.is_amsterdam_active_at_timestamp(0));
    }

    #[test]
    fn cancun_activated_at_genesis() {
        use reth_chainspec::EthereumHardforks;
        let spec = dev_chainspec();
        assert!(
            spec.is_cancun_active_at_timestamp(0),
            "Cancun should be active at genesis"
        );
    }
}

