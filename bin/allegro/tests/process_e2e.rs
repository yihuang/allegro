//! Process-level e2e test for the allegro binary.
//!
//! Starts multiple instances, checks they start and connect.

use std::process::{Command, Stdio};
use std::time::Duration;

/// Path to the allegro binary, resolved at runtime so the test still finds it when
/// run from a `cargo nextest archive` on another machine (`env!` would hardcode the
/// build path). Falls back to Cargo's var outside nextest.
fn binary() -> String {
    std::env::var("NEXTEST_BIN_EXE_allegro")
        .or_else(|_| std::env::var("CARGO_BIN_EXE_allegro"))
        .expect("neither NEXTEST_BIN_EXE_allegro nor CARGO_BIN_EXE_allegro is set")
}

#[test]
fn test_allegro_binary_help() {
    let output = Command::new(binary())
        .arg("--help")
        .output()
        .expect("failed to run allegro");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("allegro"));
    assert!(stdout.contains("--listen"));
    assert!(stdout.contains("--peer"));
}

#[test]
fn test_allegro_binary_version() {
    let output = Command::new(binary())
        .arg("--version")
        .output()
        .expect("failed to run allegro");
    assert!(output.status.success());
}

#[test]
fn test_allegro_single_node_start() {
    let mut child = Command::new(binary())
        .arg("--node")
        .arg("0")
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--leader-timeout")
        .arg("500")
        .arg("--cert-timeout")
        .arg("1000")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start allegro");

    // Let it run briefly
    std::thread::sleep(Duration::from_secs(2));

    // Check it's still alive
    match child.try_wait() {
        Ok(Some(status)) => panic!("allegro exited early: {status}"),
        Ok(None) => {
            // Still running — kill it
            child.kill().ok();
            let _ = child.wait();
        }
        Err(e) => panic!("error checking child: {e}"),
    }
}

/// Pick a free TCP port by binding to port 0 temporarily.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.port()
}

/// Minimal JSON-RPC `eth_blockNumber` client over raw TCP (no extra deps).
fn rpc_block_number(port: u16) -> Option<u64> {
    use std::io::{Read, Write};

    let body = r#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"#;
    let req = format!(
        "POST / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(req.as_bytes()).ok()?;

    let mut resp = String::new();
    stream.read_to_string(&mut resp).ok()?;

    // Extract `"result":"0x..."` from the JSON body.
    let marker = "\"result\":\"";
    let start = resp.find(marker)? + marker.len();
    let end = resp[start..].find('"')? + start;
    u64::from_str_radix(resp[start..end].trim_start_matches("0x"), 16).ok()
}

/// Poll `eth_blockNumber` until it reaches `target` (or timeout).
fn wait_for_block(port: u16, target: u64, timeout: Duration) -> Result<u64, String> {
    let start = std::time::Instant::now();
    loop {
        if let Some(n) = rpc_block_number(port) {
            if n >= target {
                return Ok(n);
            }
        }
        if start.elapsed() > timeout {
            return Err(format!(
                "port {port}: block number did not reach {target} within {timeout:?} (last: {:?})",
                rpc_block_number(port)
            ));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn test_allegro_two_nodes() {
    let port0 = free_port();
    let port1 = free_port();
    let rpc0 = free_port();
    let rpc1 = free_port();

    eprintln!("ports: {port0}, {port1}; rpc: {rpc0}, {rpc1}");

    // Each embedded reth node needs its own datadir and p2p/rpc/authrpc ports.
    let datadir0 = std::env::temp_dir().join(format!("allegro-e2e-n0-{port0}"));
    let datadir1 = std::env::temp_dir().join(format!("allegro-e2e-n1-{port1}"));
    let _ = std::fs::remove_dir_all(&datadir0);
    let _ = std::fs::remove_dir_all(&datadir1);

    let spawn_node =
        |node: u8, listen: u16, rpc_port: u16, peers: &[String], datadir: &std::path::Path| {
            let mut cmd = Command::new(binary());
            cmd.arg("--node")
                .arg(node.to_string())
                .arg("--listen")
                .arg(format!("127.0.0.1:{listen}"))
                .arg("--datadir")
                .arg(datadir)
                .arg("--rpc-port")
                .arg(rpc_port.to_string())
                .arg("--authrpc-port")
                .arg(free_port().to_string())
                .arg("--reth-p2p-port")
                .arg(free_port().to_string());

            for peer in peers {
                cmd.arg("--peer").arg(peer);
            }
            cmd.stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap_or_else(|e| panic!("failed to start node {node}: {e}"))
        };

    // Start node 0 — must also reference node 1 for mutual peer tracking.
    let node1_addr = format!("127.0.0.1:{port1}");
    let mut node0 = spawn_node(0, port0, rpc0, std::slice::from_ref(&node1_addr), &datadir0);

    std::thread::sleep(Duration::from_millis(200));

    // Start node 1, connecting to node 0
    let node0_addr = format!("127.0.0.1:{port0}");
    let mut node1 = spawn_node(1, port1, rpc1, &[node0_addr], &datadir1);

    // Wait for both nodes to produce/finalize blocks: node 0 proposes even
    // views, node 1 odd views; both must advance via consensus + reth FCU.
    let result0 = wait_for_block(rpc0, 2, Duration::from_secs(60));
    let result1 = wait_for_block(rpc1, 2, Duration::from_secs(60));

    // Check both are still alive
    let mut failed = result0.is_err() || result1.is_err();
    eprintln!("node0 block: {result0:?}, node1 block: {result1:?}");
    for (i, child) in [&mut node0, &mut node1].iter_mut().enumerate() {
        let status = child.try_wait().expect("check child");
        if let Some(code) = status {
            eprintln!("node {i} exited early with code {code}");
            failed = true;
        }
    }

    // From here on the children are consumed by value.
    for (i, mut child) in [node0, node1].into_iter().enumerate() {
        child.kill().ok();
        let output = child.wait_with_output().expect("wait for child");
        if failed {
            eprintln!(
                "── node {i} stdout ──\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
            eprintln!(
                "── node {i} stderr ──\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    let _ = std::fs::remove_dir_all(&datadir0);
    let _ = std::fs::remove_dir_all(&datadir1);

    assert!(result0.is_ok(), "node 0: {}", result0.unwrap_err());
    assert!(result1.is_ok(), "node 1: {}", result1.unwrap_err());
    assert!(!failed, "one or more nodes exited early");
}
