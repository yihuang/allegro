//! allegro-xtask — genesis and devnet utilities.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use clap::Parser;
use commonware_cryptography::{Signer as _, ed25519::PrivateKey};
use eyre::WrapErr as _;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "allegro-xtask")]
enum Cli {
    /// Generate genesis config + validator keys for a devnet.
    Genesis(GenesisCmd),
}

#[derive(Debug, clap::Args)]
struct GenesisCmd {
    /// Number of validators.
    #[arg(long, default_value = "4")]
    validators: u16,

    /// Starting P2P port (each validator gets port+index).
    #[arg(long, default_value = "13000")]
    base_port: u16,

    /// Output directory.
    #[arg(short, long, default_value = "./devnet")]
    output: PathBuf,
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

        // Generate N validator entries
        let mut validators = Vec::new();
        for i in 0..self.validators {
            let seed = i as u64;
            let sk = PrivateKey::from_seed(seed);
            let pk = sk.public_key();

            let ingress = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.base_port + i);

            validators.push(ValidatorOutput {
                index: i,
                public_key: hex::encode(pk.as_ref()),
                private_key_seed: seed,
                ingress: ingress.to_string(),
                egress: Ipv4Addr::LOCALHOST.to_string(),
            });
        }

        // Write validators.json
        let val_path = output.join("validators.json");
        let val_json = serde_json::to_string_pretty(&validators)
            .wrap_err("serialize validators")?;
        std::fs::write(&val_path, &val_json)
            .wrap_err_with(|| format!("write {}", val_path.display()))?;
        println!("wrote {} validators to {}", validators.len(), val_path.display());

        // Write genesis.json (minimal Ethereum dev genesis)
        let genesis = GenesisConfig::dev(validators.iter().map(|v| &v.public_key));
        let genesis_path = output.join("genesis.json");
        let genesis_json = serde_json::to_string_pretty(&genesis)
            .wrap_err("serialize genesis")?;
        std::fs::write(&genesis_path, &genesis_json)
            .wrap_err_with(|| format!("write {}", genesis_path.display()))?;
        println!("wrote genesis to {}", genesis_path.display());

        // Write node configs
        for v in &validators {
            let node_dir = output.join(format!("node-{}", v.index));
            std::fs::create_dir_all(&node_dir)
                .wrap_err_with(|| format!("create {}", node_dir.display()))?;

            // Write separate signing key file
            let key_path = node_dir.join("signing_key.hex");
            std::fs::write(&key_path, format!("0x{:064x}", v.private_key_seed))
                .wrap_err_with(|| format!("write {}", key_path.display()))?;
        }

        println!("\nTo start a node:\n  allegro --node 0 --listen 127.0.0.1:13000");
        println!("  allegro --node 1 --listen 127.0.0.1:13001 --peer 127.0.0.1:13000");

        Ok(())
    }
}

// ── Output types ─────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct ValidatorOutput {
    index: u16,
    public_key: String,
    private_key_seed: u64,
    ingress: String,
    egress: String,
}

#[derive(Debug, Serialize)]
struct GenesisConfig(serde_json::Value);

impl GenesisConfig {
    /// Minimal Ethereum dev genesis.
    fn dev<'a>(_validators: impl Iterator<Item = &'a String>) -> Self {
        // Standard Reth dev genesis
        let json = serde_json::json!({
            "config": {
                "chainId": 1337,
                "homesteadBlock": 0,
                "eip150Block": 0,
                "eip155Block": 0,
                "eip158Block": 0,
                "byzantiumBlock": 0,
                "constantinopleBlock": 0,
                "petersburgBlock": 0,
                "istanbulBlock": 0,
                "berlinBlock": 0,
                "londonBlock": 0,
                "mergeNetsplitBlock": 0,
                "shanghaiTime": 0,
                "cancunTime": 0,
                "terminalTotalDifficulty": 0,
                "terminalTotalDifficultyPassed": true
            },
            "nonce": "0x0",
            "timestamp": "0x0",
            "extraData": "0x",
            "gasLimit": "0x1c9c380",
            "difficulty": "0x0",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "coinbase": "0x0000000000000000000000000000000000000000",
            "alloc": {}
        });
        Self(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xtask_generates_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = GenesisCmd {
            validators: 2,
            base_port: 13000,
            output: dir.path().to_path_buf(),
        };
        cmd.run().unwrap();

        let val_path = dir.path().join("validators.json");
        let content = std::fs::read_to_string(&val_path).unwrap();
        let vals: Vec<ValidatorOutput> = serde_json::from_str(&content).unwrap();
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0].index, 0);
        assert_eq!(vals[1].index, 1);
        assert_ne!(vals[0].public_key, vals[1].public_key);

        let genesis_path = dir.path().join("genesis.json");
        assert!(genesis_path.exists());
    }
}
