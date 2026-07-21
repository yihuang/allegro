//! Allegro — minimal Reth + Commonware consensus node.
//!
//! This binary is the entry point for the Allegro node.
//! Currently in early development — run with `--help` to see available options.

use std::net::SocketAddr;

use clap::Parser;
use commonware_cryptography::{Signer as _, ed25519::PrivateKey};
use eyre::WrapErr as _;
use tracing::info;

/// Allegro — minimal Reth + Commonware consensus node.
#[derive(Debug, Parser)]
#[command(name = "allegro", version)]
pub struct AllegroCli {
    /// Path to the Ed25519 signing key (hex-encoded 32-byte private key).
    #[arg(long = "signing-key", env = "ALLEGRO_SIGNING_KEY")]
    pub signing_key: Option<String>,

    /// P2P listen address.
    #[arg(long = "listen-address", default_value = "0.0.0.0:3000")]
    pub listen_address: SocketAddr,

    /// Seed for deterministic key generation (dev only, u64).
    #[arg(long = "dev-key-seed")]
    pub dev_key_seed: Option<u64>,

    /// Path to the validator set JSON file.
    #[arg(long = "validators")]
    pub validators_path: Option<String>,
}

fn load_signing_key(key_hex: Option<&str>, dev_seed: Option<u64>) -> eyre::Result<PrivateKey> {
    if let Some(seed) = dev_seed {
        return Ok(PrivateKey::from_seed(seed));
    }

    if let Some(hex_key) = key_hex {
        let key = hex_key.strip_prefix("0x").unwrap_or(hex_key);
        let bytes = hex::decode(key).wrap_err("signing key must be valid hex")?;
        if bytes.len() != 32 {
            eyre::bail!("signing key must be 32 bytes (64 hex chars)");
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        // Construct from raw 32-byte seed
        let seed = u64::from_le_bytes(arr[..8].try_into().unwrap());
        return Ok(PrivateKey::from_seed(seed));
    }

    // Generate a random key from system time + PID
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    let seed = nanos.wrapping_mul(6364136223846793005).wrapping_add(pid);
    info!(seed, "generated random dev signing key");
    Ok(PrivateKey::from_seed(seed))
}

fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = AllegroCli::parse();
    let signing_key = load_signing_key(
        cli.signing_key.as_deref(),
        cli.dev_key_seed,
    )?;
    let public_key = signing_key.public_key();

    info!(
        %public_key,
        listen_address = %cli.listen_address,
        "Allegro node starting",
    );

    // TODO: launch Reth execution layer + Commonware consensus engine
    eprintln!("Allegro node — work in progress");
    eprintln!("See: crates/primitives (foundational types — done)");
    eprintln!("     crates/consensus (commonware integration — skeleton)");
    eprintln!("     crates/node       (reth node types — placeholder)");

    Ok(())
}
