//! Chainspec helpers for the Allegro devnet.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use alloy_genesis::Genesis;
use allegro_consensus::{ValidatorEntry, ValidatorSet};
use commonware_codec::DecodeExt;
use commonware_cryptography::ed25519::PublicKey;
use reth_chainspec::ChainSpec;
use serde::Deserialize;

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
    let (spec, _) = load_chain_with_validators(path)?;
    Ok(spec)
}

// ── Validator-file entry (matches xtask output) ─────────────

/// A single validator entry in the validators.json / genesis.json format.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidatorFileEntry {
    #[allow(dead_code)]
    index: u16,
    public_key: String,
    ingress: String,
}

/// Load chain spec and optionally embedded validators from a genesis JSON file.
///
/// The file can be either:
/// - **New format**: an `AllegroGenesis` with a top-level `"validators"` array
/// - **Old format**: a plain `Genesis` (validators will be `None`)
///
/// This is the single entry-point for `--genesis` loading; the binary calls this
/// and then uses the returned validators (or falls back to `--peer` derivation).
pub fn load_chain_with_validators(path: &Path) -> eyre::Result<(Arc<ChainSpec>, Option<ValidatorSet>)> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| eyre::eyre!("read genesis file {}: {e}", path.display()))?;

    // Parse as generic JSON, extract "validators" if present
    let mut value: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| eyre::eyre!("parse genesis JSON {}: {e}", path.display()))?;

    let validators = value.as_object_mut()
        .and_then(|obj| obj.remove("validators"))
        .map(|v| serde_json::from_value::<Vec<ValidatorFileEntry>>(v))
        .transpose()
        .map_err(|e| eyre::eyre!("parse validators in genesis {}: {e}", path.display()))?;

    // Deserialize the rest as a standard Genesis
    let genesis: Genesis = serde_json::from_value(value)
        .map_err(|e| eyre::eyre!("parse genesis fields {}: {e}", path.display()))?;

    let chain_spec = Arc::new(ChainSpec::from_genesis(genesis));

    let validator_set = validators.map(|entries| {
        let entries: Vec<ValidatorEntry> = entries
            .iter()
            .map(|e| {
                let pk_bytes = hex::decode(&e.public_key).unwrap_or_else(|_| {
                    panic!(
                        "invalid hex public_key for validator {}: {}",
                        e.index, e.public_key
                    )
                });
                let pk = PublicKey::decode(pk_bytes.as_ref()).unwrap_or_else(|err| {
                    panic!(
                        "invalid public_key bytes for validator {}: {err}",
                        e.index
                    )
                });
                let ingress: SocketAddr = e.ingress.parse().unwrap_or_else(|_| {
                    panic!(
                        "invalid ingress address for validator {}: {}",
                        e.index, e.ingress
                    )
                });
                ValidatorEntry {
                    public_key: pk,
                    ingress,
                    egress: ingress.ip(),
                }
            })
            .collect();
        ValidatorSet::from_entries(&entries)
    });

    Ok((chain_spec, validator_set))
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
