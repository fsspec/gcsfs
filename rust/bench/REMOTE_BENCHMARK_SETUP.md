# Remote VM Benchmarking & Zero-Touch SSH Setup

Guide to setting up fast, reproducible remote benchmarking for `gcsfs` and the Rust SDK backend without requiring physical security key taps on every command.

---

## 1. Why SSH Prompts for Security Key Multiple Times

On Google workstations, `ssh-agent` loads hardware security key certificates (e.g. `corp/normal`, `prod/normal`). When connecting to a remote host without pinning the identity:

1. OpenSSH iterates through each certificate in `ssh-agent`.
2. Each hardware key certificate prompts for physical presence (YubiKey/Titan key touch).
3. If multiple certificates are tried before authenticating, you must tap the key multiple times per connection.

---

## 2. The Zero-Touch Solution

To eliminate all security key touches and make connection latency near zero:

1. **Authorize a local file-based SSH key** on the remote VM.
2. **Force `IdentitiesOnly yes`** in `~/.ssh/config` so SSH never asks `ssh-agent` for hardware keys.
3. **Enable SSH multiplexing (`ControlPersist`)** so subsequent commands reuse the open connection socket.

---

## 3. Step-by-Step Setup

### Step 1: Copy Your File-Based Key to the Remote VM

Run this command once from your local workstation:

```bash
cat ~/.ssh/google_compute_engine.pub | ssh princer_google_com@<VM_HOSTNAME> \
  "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

*(Replace `<VM_HOSTNAME>` with your remote VM hostname or IP, e.g. `nic0.princer-us-west4a.us-west4-a.c.gcs-aiml-clients-testing-101.internal.gcpnode.com`).*

---

### Step 2: Configure `~/.ssh/config` on Your Local Machine

1. Create the sockets directory:
   ```bash
   mkdir -p ~/.ssh/sockets
   ```

2. Add this entry to `~/.ssh/config`:
   ```ssh-config
   Host princer-vm
       HostName nic0.princer-us-west4a.us-west4-a.c.gcs-aiml-clients-testing-101.internal.gcpnode.com
       User princer_google_com
       
       # 1. Bypass hardware security keys in ssh-agent
       IdentityFile ~/.ssh/google_compute_engine
       IdentitiesOnly yes
       StrictHostKeyChecking accept-new
       
       # 2. Connection multiplexing (never times out, instant connection)
       ControlMaster auto
       ControlPath ~/.ssh/sockets/%C
       ControlPersist yes
       ServerAliveInterval 15
       ServerAliveCountMax 240
       TCPKeepAlive yes
   ```

> **Note on `%C`:** Linux limits UNIX domain socket paths to 108 bytes. Using `%C` generates a compact SHA1 hash of the connection parameters, preventing `ControlPath too long` errors.

---

### Step 3: Verify Zero-Touch Connection

Test with `BatchMode=yes` (which fails immediately if any password or physical key interaction is needed):

```bash
ssh -o BatchMode=yes princer-vm "echo 'Zero-touch SSH working!'"
```

---

## 4. Setting Up the Remote Workspace

SSH into the VM and set up the dependencies:

```bash
ssh princer-vm

# 1. System dependencies
sudo apt update && sudo apt install -y build-essential time git python3-venv python3-pip curl

# 2. Install Rust toolchain (if not installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# 3. Create Python virtualenv & clone repo
mkdir -p ~/dev && cd ~/dev
git clone -b with_rust_sdk https://github.com/raj-prince/gcsfs.git
cd gcsfs
python3 -m venv .env
source .env/bin/activate
pip install --upgrade pip maturin fsspec aiohttp
pip install -e .

# 4. Build the PyO3 Rust extension
cd rust/gcsfs_rust
maturin develop --release
cd ../..
```

---

## 5. Workflow Helpers (`~/.vm_helpers.sh`)

Add the following utilities to `~/.vm_helpers.sh` on your local machine and source it in `~/.bashrc` / `~/.zshrc`:

```bash
[ -f "$HOME/.vm_helpers.sh" ] && source "$HOME/.vm_helpers.sh"
```

### Content of `~/.vm_helpers.sh`:

```bash
#!/usr/bin/env bash

VM_HOST="${VM_HOST:-princer-vm}"
LOCAL_GCSFS_DIR="${LOCAL_GCSFS_DIR:-/usr/local/google/home/princer/code/ws1/gcsfs}"
REMOTE_GCSFS_DIR="${REMOTE_GCSFS_DIR:-~/dev/gcsfs}"

# 1. Pure file sync (instant, <1s)
vm-sync() {
  local host="${1:-$VM_HOST}"
  echo "==> Syncing workspace from $LOCAL_GCSFS_DIR to $host:$REMOTE_GCSFS_DIR..."
  rsync -avz \
    --exclude '.git' \
    --exclude '.venv' \
    --exclude '.env' \
    --exclude 'target' \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude '.pytest_cache' \
    "$LOCAL_GCSFS_DIR/" "$host:$REMOTE_GCSFS_DIR/"
}

# 2. Rebuild Rust extension on the VM (only when Rust code changes)
vm-build() {
  local host="${1:-$VM_HOST}"
  echo "==> Building PyO3 extension on $host..."
  ssh "$host" "bash -l -c '
    cd $REMOTE_GCSFS_DIR
    source .env/bin/activate 2>/dev/null || source .venv/bin/activate 2>/dev/null
    source \$HOME/.cargo/env 2>/dev/null || true
    cd rust/gcsfs_rust
    maturin develop --release
  '"
}

# 3. Run any command on the VM inside the active environment
vm-run() {
  local host="$VM_HOST"
  local cmd="$*"
  if [[ -z "$cmd" ]]; then
    echo "Usage: vm-run <command...>"
    return 1
  fi

  ssh -t "$host" "bash -l -c '
    cd $REMOTE_GCSFS_DIR
    source .env/bin/activate 2>/dev/null || source .venv/bin/activate 2>/dev/null
    source \$HOME/.cargo/env 2>/dev/null || true
    $cmd
  '"
}
```

---

## 6. Daily Usage Cheat Sheet

```bash
# Sync local changes to VM
vm-sync

# Rebuild only if you touched Rust code
vm-build

# Run the backend comparison benchmark
vm-run ./rust/bench/compare_backends.sh gs://princer-bucket/10gfile.bin

# Run with custom parameters (e.g. 10 runs, concurrency=32)
vm-run RUNS=10 CONCURRENCY=32 MALLOC_ARENA_MAX=4 ./rust/bench/compare_backends.sh gs://princer-bucket/10gfile.bin

# Run standalone Rust binary (no Python)
vm-run cargo run --release -p bench_rust_read -- princer-bucket 10gfile.bin 10737418240 16
```
