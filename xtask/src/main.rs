//! allegro-xtask — genesis config generator for devnets.
//!
//! Generates a `genesis.json` consumable by the allegro binary (`--chain` flag),
//! along with a validators manifest.
//!
//! Funded accounts are derived from the standard Anvil mnemonic:
//!   "test test test test test test test test test test test junk"
//! at BIP-44 Ethereum derivation path m/44'/60'/0'/0/{i}.  This matches what
//! `cast send` and forge scripts expect out of the box.

use std::collections::BTreeMap;
use std::path::PathBuf;

use alloy_genesis::{ChainConfig, Genesis, GenesisAccount};
use alloy_primitives::{Address, B256, U256};
use alloy_signer_local::MnemonicBuilder;
use clap::Parser;
use commonware_cryptography::ed25519::PrivateKey;
use commonware_cryptography::Signer as _;
use eyre::WrapErr;
use serde::{Deserialize, Serialize};

/// The standard Anvil mnemonic used across the Ethereum tooling ecosystem.
const ANVIL_MNEMONIC: &str = "test test test test test test test test test test test junk";

/// Number of prefunded accounts to create (20 = same as anvil / DEV spec).
const PREFUND_COUNT: u32 = 20;

/// CLI definition.
#[derive(Debug, Parser)]
#[command(name = "allegro-xtask")]
enum Cli {
    /// Generate devnet genesis and validator configs.
    Genesis(GenesisCmd),
}

#[derive(Debug, clap::Args)]
struct GenesisCmd {
    /// Number of validators, or a comma-separated list of `ip:port` consensus
    /// sockets (one per validator, e.g. `127.0.0.1:8000,127.0.0.1:8010`).
    #[arg(long, default_value = "4")]
    validators: String,
    /// Base port for p2p when `--validators` is a count (each validator gets
    /// port+index); ignored when explicit sockets are given.
    #[arg(long, default_value = "13000")]
    base_port: u16,
    /// Chain ID for the genesis config.
    #[arg(long, default_value = "1337")]
    chain_id: u64,
    /// Output directory.
    #[arg(short, long, default_value = "./devnet")]
    output: PathBuf,
}

// ── Output types ───────────────────────────────────────────

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidatorOutput {
    index: u16,
    public_key: String,
    ingress: String,
    egress: String,
}

fn main() -> eyre::Result<()> {
    match Cli::parse() {
        Cli::Genesis(cmd) => cmd.run(),
    }
}

impl GenesisCmd {
    fn run(self) -> eyre::Result<()> {
        let output = &self.output;
        std::fs::create_dir_all(output)
            .wrap_err_with(|| format!("failed to create {}", output.display()))?;

        // ── Generate validators ──
        // `--validators` is either a count (sockets derived from --base-port)
        // or an explicit comma-separated `ip:port` list.
        let sockets: Vec<String> = match self.validators.parse::<u16>() {
            Ok(n) => (0..n)
                .map(|i| format!("127.0.0.1:{}", self.base_port + i))
                .collect(),
            Err(_) => self
                .validators
                .split(',')
                .map(|s| {
                    let s = s.trim();
                    s.parse::<std::net::SocketAddr>()
                        .map(|addr| addr.to_string())
                        .wrap_err_with(|| format!("invalid validator socket {s:?}"))
                })
                .collect::<eyre::Result<_>>()?,
        };
        let mut validators = Vec::new();
        for (i, socket) in sockets.iter().enumerate() {
            let i = i as u16;
            let sk = PrivateKey::from_seed(i as u64);
            let pk = sk.public_key();
            validators.push(ValidatorOutput {
                index: i,
                public_key: hex::encode(pk.as_ref()),
                ingress: socket.clone(),
                egress: socket.clone(),
            });
        }

        // ── Generate funded accounts via HD derivation ──
        let mut alloc = BTreeMap::new();
        for i in 0..PREFUND_COUNT {
            let signer = MnemonicBuilder::try_from_phrase_nth(ANVIL_MNEMONIC, i)
                .wrap_err_with(|| format!("derive account {i} from mnemonic"))?;

            let addr: Address = signer.address();

            alloc.insert(
                addr,
                GenesisAccount {
                    balance: U256::from(10_000_000_000_000_000_000_000u128), // 10_000 ETH
                    ..Default::default()
                },
            );
        }

        // ── Build ChainConfig (all forks at genesis, like DEV spec) ──
        let config = ChainConfig {
            chain_id: self.chain_id,
            // All block-number-based forks at 0
            homestead_block: Some(0),
            eip150_block: Some(0),
            eip155_block: Some(0),
            eip158_block: Some(0),
            byzantium_block: Some(0),
            constantinople_block: Some(0),
            petersburg_block: Some(0),
            istanbul_block: Some(0),
            muir_glacier_block: Some(0),
            berlin_block: Some(0),
            london_block: Some(0),
            arrow_glacier_block: Some(0),
            gray_glacier_block: Some(0),
            // Merge at genesis
            terminal_total_difficulty: Some(U256::ZERO),
            merge_netsplit_block: None,
            // Fork-timestamp-based forks at 0 (Shanghai through Osaka)
            shanghai_time: Some(0),
            cancun_time: Some(0),
            prague_time: Some(0),
            osaka_time: Some(0),
            amsterdam_time: None,
            ..Default::default()
        };

        let genesis = Genesis {
            config,
            nonce: 0x42,
            timestamp: 0,
            extra_data: Default::default(),
            gas_limit: 30_000_000,
            difficulty: U256::ZERO,
            mix_hash: B256::ZERO,
            coinbase: Address::ZERO,
            alloc,
            base_fee_per_gas: None,
            excess_blob_gas: None,
            blob_gas_used: None,
            number: None,
            parent_hash: None,
        };

        // ── Write genesis.json with validators embedded ──
        let genesis_path = output.join("genesis.json");
        let mut genesis_value = serde_json::to_value(&genesis).wrap_err("serialize genesis")?;
        // Inject validators as a top-level field
        let validators_value =
            serde_json::to_value(&validators).wrap_err("serialize validators")?;
        if let Some(obj) = genesis_value.as_object_mut() {
            obj.insert("validators".to_string(), validators_value);
        }
        let genesis_json =
            serde_json::to_string_pretty(&genesis_value).wrap_err("pretty genesis")?;
        std::fs::write(&genesis_path, &genesis_json)
            .wrap_err_with(|| format!("write {}", genesis_path.display()))?;
        println!(
            "wrote genesis (with {} validators) to {}",
            validators.len(),
            genesis_path.display()
        );

        // Also write a standalone validators.json for reference / backward compat
        let val_path = output.join("validators.json");
        let val_json =
            serde_json::to_string_pretty(&validators).wrap_err("serialize validators")?;
        std::fs::write(&val_path, &val_json)
            .wrap_err_with(|| format!("write {}", val_path.display()))?;
        println!(
            "wrote {} validators to {} (reference)",
            validators.len(),
            val_path.display()
        );

        println!();
        println!("To start a node (validators and their peers are embedded in genesis.json):");
        for v in &validators {
            println!(
                "  allegro node --chain {} --consensus.node-index {} --consensus.listen-address {}",
                genesis_path.display(),
                v.index,
                v.ingress,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xtask_accepts_explicit_validator_sockets() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = GenesisCmd {
            validators: "127.0.0.1:8000,127.0.0.1:8010".to_string(),
            base_port: 13000,
            output: dir.path().to_path_buf(),
            chain_id: 1337,
        };
        cmd.run().unwrap();
        let content = std::fs::read_to_string(dir.path().join("genesis.json")).unwrap();
        let value: serde_json::Value = serde_json::from_str(&content).unwrap();
        let validators = value["validators"].as_array().unwrap();
        assert_eq!(validators.len(), 2);
        assert_eq!(validators[0]["ingress"], "127.0.0.1:8000");
        assert_eq!(validators[1]["ingress"], "127.0.0.1:8010");
    }

    #[test]
    fn xtask_generates_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = GenesisCmd {
            validators: "2".to_string(),
            base_port: 13000,
            output: dir.path().to_path_buf(),
            chain_id: 1337,
        };
        cmd.run().unwrap();
        let content = std::fs::read_to_string(dir.path().join("genesis.json")).unwrap();

        // 1) Standard Genesis fields should parse correctly
        let genesis: Genesis = serde_json::from_str(&content).unwrap();
        assert_eq!(genesis.config.chain_id, 1337);
        assert!(genesis.config.shanghai_time == Some(0));
        assert_eq!(genesis.alloc.len(), PREFUND_COUNT as usize);

        // 2) Validators should be embedded at the top level
        let value: serde_json::Value = serde_json::from_str(&content).unwrap();
        let validators = value
            .as_object()
            .and_then(|obj| obj.get("validators"))
            .and_then(|v| v.as_array())
            .expect("genesis.json should have a top-level 'validators' array");
        assert_eq!(validators.len(), 2, "should have 2 validators");
        assert!(
            validators[0].as_object().unwrap().contains_key("publicKey"),
            "validator entry should have publicKey"
        );
        assert!(
            validators[0].as_object().unwrap().contains_key("ingress"),
            "validator entry should have ingress"
        );
        assert!(
            validators[0].as_object().unwrap().contains_key("egress"),
            "validator entry should have egress"
        );
    }

    #[test]
    fn derived_keys_match_anvil_defaults() {
        // Account #0 from the anvil mnemonic should have the well-known key.
        let signer = MnemonicBuilder::from_phrase_nth(ANVIL_MNEMONIC, 0);
        assert_eq!(
            signer.to_bytes(),
            B256::from_slice(
                &hex::decode("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
                    .unwrap()
            )
        );
        assert_eq!(
            signer.address(),
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
                .parse::<Address>()
                .unwrap()
        );
    }
}
