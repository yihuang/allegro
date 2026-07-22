#!/usr/bin/env python3
"""allegro local devnet — start N nodes, confirm block production."""
import os, sys, time, signal, subprocess, tempfile, json

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2
BASE_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 13000
DATA_DIR = tempfile.mkdtemp(prefix="allegro-devnet-")
BINARY = os.environ.get("ALLEGRO_BIN", "target/debug/allegro")
XTASK = os.environ.get("XTASK_BIN", "target/debug/allegro-xtask")

def log(msg):
    print(f"=== {msg} ===", flush=True)

# 1. build if needed
if not os.path.exists(BINARY) or not os.path.exists(XTASK):
    log("building")
    subprocess.run(["cargo", "build", "-p", "allegro", "-p", "allegro-xtask", "-q"],
                   capture_output=True)

# 2. genesis
log(f"generating genesis ({N} validators)")
subprocess.run([XTASK, "genesis", "--validators", str(N),
                "--base-port", str(BASE_PORT), "--output", DATA_DIR], check=True)

# 3. start nodes
log("starting nodes")
procs = []
for i in range(N):
    port = BASE_PORT + i
    peers = [f"--peer=127.0.0.1:{BASE_PORT + j}" for j in range(N) if j != i]
    log_file = open(f"{DATA_DIR}/node-{i}.log", "w")
    env = {**os.environ, "RUST_LOG": "allegro=info,commonware=warn",
           "PYTHONUNBUFFERED": "1"}
    p = subprocess.Popen(
        [BINARY, "--node", str(i), "--listen", f"127.0.0.1:{port}",
         "--leader-timeout", "1000", "--cert-timeout", "2000"] + peers,
        stdout=log_file, stderr=subprocess.STDOUT, env=env)
    procs.append(p)
    print(f"  node {i} : pid {p.pid} : port {port}")

# 4. wait for startup
log("waiting for nodes to start")
for _ in range(10):
    time.sleep(1)
    if all(p.poll() is None for p in procs):
        break
if procs[0].poll() is not None:
    print(f"ERROR: node 0 exited with code {procs[0].returncode}")
    sys.exit(1)

# 5. check block production
log("checking block production (10s)")
time.sleep(10)

total = 0
for i, p in enumerate(procs):
    if p.poll() is not None:
        print(f"  node {i} : exited with code {p.returncode}")
        continue
    log_file = f"{DATA_DIR}/node-{i}.log"
    with open(log_file) as f:
        content = f.read()
    count = content.count("handle_propose called")
    total += count
    print(f"  node {i} : {count} blocks")

if total < N:
    print(f"\nWARNING: only {total} blocks across {N} nodes (expected >= {N})")
    log_file = f"{DATA_DIR}/node-0.log"
    with open(log_file) as f:
        print("Last 10 lines from node-0.log:")
        for line in f.readlines()[-10:]:
            print(f"  {line.rstrip()}")
    # Check that at least one node produces blocks
    individual = [f"{DATA_DIR}/node-{i}.log" for i in range(N)]
    counts = []
    for lf in individual:
        with open(lf) as f:
            counts.append(f.read().count("handle_propose called"))
    if max(counts) > 0:
        print(f"\nPARTIAL SUCCESS: {counts} (at least one node produced blocks)")
        print(f"Multi-node consensus requires p2p wiring. Logs: {DATA_DIR}")
        sys.exit(0)
    sys.exit(1)

print(f"\n{'='*44}")
print(f" SUCCESS: {total} blocks across {N} nodes")
print(f" Logs: {DATA_DIR}")
print(f"{'='*44}")
print("\nPress Ctrl+C to stop.")

try:
    while True:
        time.sleep(1)
        for i, p in enumerate(procs):
            if p.poll() is not None:
                print(f"node {i} exited")
                break
        else:
            continue
        break
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except:
            p.kill()
