//! Chainspec helpers for the Allegro devnet.

use std::path::Path;
use std::sync::Arc;

use alloy_genesis::Genesis;
use reth_chainspec::ChainSpec;

/// Return the DEV chainspec, which activates all hardforks through Osaka at genesis
/// and includes 20 prefunded accounts derived from the "test test test ... junk" mnemonic.
///
/// This is the default chainspec used when no `--genesis` path is provided.
pub fn dev_chainspec() -> Arc<ChainSpec> {
    reth_chainspec::DEV.clone()
}

/// Load a `ChainSpec` from a genesis JSON file (alloy Genesis format).
///
/// The JSON should match the format produced by `allegro-xtask genesis`.
pub fn chain_spec_from_genesis_json(path: &Path) -> eyre::Result<Arc<ChainSpec>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| eyre::eyre!("read genesis file {}: {e}", path.display()))?;
    let genesis: Genesis = serde_json::from_str(&content)
        .map_err(|e| eyre::eyre!("parse genesis JSON {}: {e}", path.display()))?;
    Ok(Arc::new(ChainSpec::from_genesis(genesis)))
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

