---
name: security-audit
description: >-
  Use this skill when asked to perform a security audit, vulnerability review,
  differential security check, threat model, or GitHub Actions / Cloud Build
  workflow security review on gcsfs (or related fsspec changes). Combines
  repository-grounded threat modeling, fail-open default detection, CPython
  ctypes/buffer auditing, auth/token boundary checks, and CI supply-chain review.
---

# GCSFS Security Audit & Threat Modeling (`security-audit`)

Perform high-signal, repository-grounded security audits and threat models on `gcsfs` without generic web-app noise (skip irrelevant XSS/CSRF/SQLi boilerplate; focus on Python cloud storage SDK, gRPC/HTTP auth, local/remote path boundaries, CPython `ctypes` memory safety, deserialization, and CI/Cloud Build pipelines).

## Required Reference

Read **[references/gcsfs_threat_surface.md](references/gcsfs_threat_surface.md)** before auditing to ground every check in `gcsfs`'s actual trust boundaries, modules, and known sharp edges.

---

## Choose the Audit Mode

### Mode 1: Differential Security Review (PR, Branch, or Uncommitted Diff)
1. Inspect changed files (`git diff <base>...<head>` or `git diff`) and read the **full surrounding functions/classes**—never audit diff hunks in isolation.
2. Classify touched surfaces against [references/gcsfs_threat_surface.md](references/gcsfs_threat_surface.md):
   - Auth, token refresh/caching, CSEK headers, CMEK `kms_key_name` fail-closed checks, `requester_pays` (`credentials.py`, `core.py`, `extended_gcsfs.py`, `zonal_file.py`)
   - REST URL construction (`quote` / `_format_path`), local path handling (`_get_file*`, caching), and Storage Insights CSV listing ingestion (`inventory_report.py`)
   - TLS/mTLS, custom `endpoint_url`, `STORAGE_EMULATOR_HOST`, `GOOGLE_CLOUD_UNIVERSE_DOMAIN` (`TPC`)
   - Zero-copy / `ctypes` / `memoryview` / async task & executor cancellation (`zb_hns_utils.py`, `zonal_file.py`, `caching.py`)
   - Pickle (`__getstate__` / `__setstate__` / `__reduce__`) & config/YAML/JSON parsing
   - `.github/workflows/`, `.github/release/`, `cloudbuild/`, `pyproject.toml`, `environment_gcsfs.yaml`
3. **Hunt for fail-open defaults & regressions**:
   - Does an `except` block widen exceptions (`except Exception:` / bare `except:`) and silently downgrade auth (`anon`), TLS, or permissions?
   - Can error/retry/cancellation paths leave partial unvalidated data, leaked credentials in tracebacks/logs, or dangling executor writes?
4. **Variant check**: When you spot a real bug pattern in the diff, grep the rest of `gcsfs/` for sibling occurrences before reporting.

### Mode 2: Subsystem Threat Modeling (New Feature or Design)
1. **Scope & separate runtime from test/CI**: Explicitly separate `gcsfs/` shipped code from `gcsfs/tests/`, `cloudbuild/`, and `.github/`.
2. **Map trust boundaries & attacker-controlled inputs**:
   - *Caller inputs*: paths, `token`, `endpoint_url`, `headers`, `cache_options`.
   - *Bucket/object inputs (often cross-tenant or untrusted)*: object names (arbitrary UTF-8 including `../`, `?`, `#`, `\x00`), object metadata/sizes, range responses, inventory reports.
   - *Network / control plane*: custom endpoints, HTTP redirects, `Retry-After` headers (not parsed today; cap if added), transient `429`/`503` responses, `BucketLayout.UNKNOWN`.
   - *Local host environment*: shared multi-user `/tmp`, `~/.gcs_tokens`, environment variables, pickle payloads across Dask/Ray/PyTorch DataLoader workers.
3. **Enumerate concrete abuse paths** (ranked by realistic impact: RCE / memory corruption -> credential or CSEK exfiltration -> arbitrary local file overwrite -> cross-bucket/API path confusion -> DoS / unbounded memory or retry amplification).
4. Produce a concise Markdown threat model with **Trust Boundaries**, **Abuse Scenarios**, **Existing Mitigations (`file:line`)**, and **Actionable Gaps**.

### Mode 3: Codebase Variant & CI Supply-Chain Audit
1. Run the targeted discovery scans in [references/gcsfs_threat_surface.md](references/gcsfs_threat_surface.md).
2. Audit `.github/workflows/*.yml` and `cloudbuild/**/*.yaml`:
   - Verify 40-char SHA pinning on third-party `uses:` actions, top-level `permissions: contents: read` (see `.github/workflows/ci.yml`), safe handling of `pull_request_target` / `workflow_run`, no direct `${{ github.event.* }}` interpolation inside `run:` blocks, and SHA-256 verification before executing downloaded tools (see `.github/workflows/bump-helm.yml`).
   - For `cloudbuild/**/*.yaml` and benchmark scripts: use pinned/versioned builder images and CLI downloads, pass `${_SUBSTITUTIONS}` via `env:` instead of splicing into `bash -c`, and use least-privilege build/VM service accounts. Keep `.pre-commit-config.yaml` hook `rev:` values pinned to 40-char SHAs.
3. Audit dependency changes in `pyproject.toml`, `environment_gcsfs.yaml`, and benchmark `requirements.txt` files for unvetted packages, yanked/non-existent versions, or unsafe install hooks.

---

## Verification & Reporting Rules

1. **Zero speculative filler**: Confirm every finding against actual code paths (trace callers, check whether `quote()` or `ssl.create_default_context()` already wraps the value upstream). Drop theoretical web-only findings that cannot occur in `gcsfs`.
2. **Every finding must include**:
   - **Severity & category** (`Critical` / `High` / `Medium` / `Hardening`)
   - **Exact location** (`[module.py](gcsfs/module.py#L12)`)
   - **Concrete trigger / exploit scenario** (3–5 lines showing exact input -> code path -> impact)
   - **Minimal, idiomatic remediation** + a **regression test sketch** (e.g., adversarial object key `"a/../b?uploadType=media"`, short-read `ctypes` bounds test, or header-redaction log assertion).
3. **When auditing as part of a PR review**: If a `pr-review` skill is active in the workspace, integrate confirmed security findings directly into that review workflow (inline on the diff line, concise and collaborative, with zero alarmist badges or emojis).
