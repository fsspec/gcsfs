# GCSFS Threat Surface & Security Audit Reference

Repository-grounded map of `gcsfs` trust boundaries, high-risk modules, known sharp edges, and targeted audit queries.

---

## 1. Trust Boundaries & Attacker-Controlled Inputs

| Boundary | Inputs Crossing Boundary | Primary Modules | Core Risk if Unvalidated |
| :--- | :--- | :--- | :--- |
| **GCS Object Keys & Metadata** | Bucket/object names, `size`, `generation`, `md5Hash`/`crc32c`, custom metadata, CSV/Parquet inventory lines | `core.py`, `extended_gcsfs.py`, `_dircache.py`, `inventory_report.py` | REST API path traversal / query injection (`?`, `#`, `/`, `..`), local directory traversal (`ZipSlip`-style `get()`), stale-`size` out-of-bounds reads |
| **Credentials, Tokens & CSEK** | `token=` (`google_default`, `cache`, `browser`, `anon`, file path, dict, raw token), OAuth refresh, CSEK headers | `credentials.py`, `core.py`, `extended_gcsfs.py` | Token/key leakage in `DEBUG` logs, exceptions, telemetry, or pickles; world-readable `~/.gcs_tokens`; silent downgrade to `anon` |
| **Transport, Endpoints & Redirects** | `endpoint_url`, `STORAGE_EMULATOR_HOST`, `GOOGLE_CLOUD_UNIVERSE_DOMAIN` (`TPC`), mTLS client certs | `core.py` (`_location`, `_client_cert_ssl_context`), `extended_gcsfs.py` | Plaintext token transmission, MITM via disabled TLS checks, private key residue in temp files, cross-host redirect token leaks |
| **CPython Zero-Copy & Async Buffers** | Multi-range download chunks (`MRD`), `ctypes.pythonapi` (`PyBytes_FromStringAndSize`), `ctypes.memmove` | `zb_hns_utils.py`, `zonal_file.py`, `caching.py` | Heap buffer overflow (`offset + size > total_size`), use-after-free if `bytes` GC'd while executor runs, leaking uninitialized heap memory on short read |
| **Serialization & Multiprocessing** | `pickle.dumps`/`loads` of `GCSFileSystem`, `GCSFile`, `ZonalFile`, `HttpError`, `~/.gcs_tokens` | `credentials.py`, `core.py`, `zonal_file.py`, `retry.py` | Arbitrary code execution via untrusted pickle files; accidental serialization of live bearer tokens into distributed task graphs (Dask/Ray) |
| **CI/CD, Release & Benchmarks** | GitHub PR metadata, `helm`/binary downloads, `gcloud`/`docker` CLI invocations, benchmark YAML configs | `.github/workflows/`, `.github/release/`, `cloudbuild/`, `gcsfs/tests/perf/` | GitHub Actions script injection, over-privileged `GITHUB_TOKEN`, unverified binary execution, `shell=True` command injection, `yaml.load` RCE |

---

## 2. Known Codebase Sharp Edges to Check First

1. **Header logging in `GCSFileSystem._call` (`gcsfs/core.py`)**:
   - `_call` logs `logger.debug(f"{method.upper()}: {path}, {args}, {kwargs.get('headers')}")` before `_request` calls `self._get_headers(...)` (which injects `Authorization`).
   - **Trap**: Any caller passing `Authorization`, `X-Goog-Encryption-Key`, `X-Goog-Copy-Source-Encryption-Key`, or signed credentials directly inside `headers=` to `_call` will leak secrets into debug logs.
2. **URL path formatting (`gcsfs/core.py` & `gcsfs/retry.py`)**:
   - `_format_path(path, args)` runs `gcsfs.core.quote(p)` (`urllib.parse.quote(s, safe="")`) *only* on positional `*args`, NOT on values already interpolated into `path`.
   - **Trap**: Writing `self._call("POST", f"b/{bucket}/o/{key}/rewriteTo/...")` instead of `self._call("POST", "b/{}/o/{}/rewriteTo/...", bucket, key)` allows `/` or `?` in `key` to hit arbitrary GCS REST endpoints or override query parameters.
3. **Raw CPython pointer writes (`gcsfs/zb_hns_utils.py`)**:
   - `PyBytes_FromStringAndSize(None, total_size)` allocates an uninitialized CPython `bytes` payload and `ctypes.memmove(self._start_address + dest_offset, data_bytes, size)` writes directly to raw memory across `ThreadPoolExecutor` threads.
   - **Traps**:
     - `PyBytes_FromStringAndSize` returns `NULL` (`MemoryError`) on huge/negative `total_size`.
     - Missing `0 <= dest_offset` or `dest_offset + size <= total_size` checks cause C-level heap corruption.
     - Cancelling async callers without waiting for in-flight executor `_do_memmove` calls can drop the last reference to `_result_bytes`, turning `_start_address` into a dangling pointer (use-after-free).
     - Returning `_result_bytes` when `total_bytes_written < total_size` exposes uninitialized heap bytes (potentially containing secrets from prior allocations) to Python callers.
4. **Legacy token cache & bare `except:` (`gcsfs/credentials.py`)**:
   - `GoogleCredentials.load_tokens` / `_save_tokens` uses `pickle` on `~/.gcs_tokens` (`tfile`), and `_dict_to_credentials` / `_connect_token` contain legacy bare `except:` blocks.
   - **Traps**: Never expand `pickle` usage to untrusted paths; ensure any touched credential file creation uses `0o600` permissions; narrow bare `except:` to specific `google.auth` / `ValueError` / `json.JSONDecodeError` exceptions so unexpected errors don't silently fall through to wrong auth modes.
5. **Rapid / Zonal eventual-consistency `size` (`gcsfs/zonal_file.py`, `gcsfs/extended_gcsfs.py`)**:
   - Metadata `size` on Zonal (`ZONAL_HIERARCHICAL`) buckets can be stale while appends are open. Using stale `size` to size raw buffers without clamping actual bytes returned can cause buffer mismatches or silent truncation.
6. **Resource & retry exhaustion (`gcsfs/poller.py`, `gcsfs/retry.py`, `gcsfs/core.py`)**:
   - Uncapped `Retry-After` headers, tight `429`/`503` polling loops without consecutive-error exponential backoff, or uncapped read buffers (`_last_read`, `MAX_PREFETCH_SIZE`) enable memory/CPU/control-plane DoS on adversarial or throttled buckets.

---

## 3. Targeted Discovery Commands (Run in Sandbox)

Use these fast `git grep` scans when auditing `gcsfs`:

```bash
# 1. Find potential unquoted REST URL f-strings in gcsfs/
git grep -n -E '(_call|_request)\([^)]*f["'\'']' -- 'gcsfs/*.py'

# 2. Check ctypes / raw memory operations and verify bounds & lifetime guards
git grep -n -E 'ctypes|pythonapi|PyBytes_|memmove|from_address' -- gcsfs/

# 3. Audit credential, header, and token logging / serialization surfaces
git grep -n -E 'Authorization|Bearer|Encryption-Key|client_secret|refresh_token|__getstate__|__reduce__|pickle\.' -- 'gcsfs/*.py' 'gcsfs/telemetry/*.py'

# 4. Check TLS, SSL, custom endpoints, and HTTP calls missing timeouts
git grep -n -E 'ssl|verify|requests\.(get|post|put|delete|head|request)|Session\(' -- 'gcsfs/*.py' '.github/' 'cloudbuild/'

# 5. Check subprocess, YAML/eval/exec, and tempfile usage across repo & CI scripts
git grep -n -E 'subprocess|shell\s*=|yaml\.load|eval\(|exec\(|mktemp|Temporary' -- gcsfs/ .github/ cloudbuild/

# 6. Audit GitHub Actions for unpinned `uses:`, permissions, and `${{ ... }}` in `run:` blocks
git grep -n -E 'uses:|permissions:|\$\{\{' -- .github/workflows/
```
