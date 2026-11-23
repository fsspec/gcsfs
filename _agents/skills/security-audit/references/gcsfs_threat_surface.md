# GCSFS Threat Surface & Security Audit Reference

Repository-grounded map of `gcsfs` trust boundaries, high-risk modules, known sharp edges, and targeted audit queries.

---

## 1. Trust Boundaries & Attacker-Controlled Inputs

| Boundary | Inputs Crossing Boundary | Primary Modules | Core Risk if Unvalidated |
| :--- | :--- | :--- | :--- |
| **GCS Object Keys & Metadata** | Bucket/object names, `size`, `generation`, `md5Hash`/`crc32c`, custom metadata, CSV inventory lines | `core.py`, `extended_gcsfs.py`, `_dircache.py`, `inventory_report.py` | REST API path traversal / query injection (`?`, `#`, `/`, `..`), local directory traversal (`ZipSlip`-style `get()`), stale-`size` out-of-bounds reads |
| **Credentials, Tokens & CSEK / CMEK** | `token=` (`google_default`, `cache`, `browser`, `cloud`, `anon`, file path, dict, raw token), OAuth refresh, CSEK headers, CMEK `kms_key_name` | `credentials.py`, `core.py`, `extended_gcsfs.py`, `zonal_file.py` | Token/key leakage in `DEBUG` logs, exceptions, telemetry, or pickles; world-readable `~/.gcs_tokens`; silent downgrade to `anon`; silent fallback to default encryption when `kms_key_name` is unsupported or mismatched |
| **Transport, Endpoints & Redirects** | `endpoint_url`, `STORAGE_EMULATOR_HOST`, `GOOGLE_CLOUD_UNIVERSE_DOMAIN` (`TPC`), mTLS client certs | `core.py` (`_location`, `_client_cert_ssl_context`), `extended_gcsfs.py` | Plaintext token transmission, MITM via disabled TLS checks, private key residue in temp files, cross-host redirect token leaks |
| **CPython Zero-Copy & Async Buffers** | Multi-range download chunks (`MRD`), `ctypes.PyDLL` (`PyBytes_*`, `PyObject_GetBuffer`/`PyBuffer_Release`), `ctypes.memmove` | `zb_hns_utils.py`, `zonal_file.py`, `caching.py` | Heap buffer overflow (`offset + size > expected_size`), use-after-free if `bytes` GC'd while executor runs, leaking uninitialized heap memory on short read |
| **Serialization & Multiprocessing** | `pickle.dumps`/`loads` of `GCSFileSystem` (`storage_options`), `GCSFile`, `ZonalFile`, `HttpError`, `~/.gcs_tokens` | `credentials.py`, `core.py`, `zonal_file.py`, `retry.py` | Arbitrary code execution via untrusted pickle files; accidental serialization of live bearer tokens (`token=`) into distributed task graphs (Dask/Ray) |
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
   - `_pythonapi = ctypes.PyDLL(...)` binds `PyBytes_FromStringAndSize` (`restype = ctypes.py_object`), `PyBytes_AsString`, `PyObject_GetBuffer`, and `PyBuffer_Release`, allocating an uninitialized `bytes` buffer written via `ctypes.memmove(self._start_address + dest_offset, view.buf, size)` across `ThreadPoolExecutor` threads.
   - **Existing guards to preserve (watch for regressions)**:
     - Bounds and non-overlap checks in `get_view` / `PartialView` (`0 <= offset`, `offset + size <= expected_size`, `_allocated_intervals`; also require `0 <= size` on any new view/slice entry point) and `self._start_address is None` checks.
     - Whole-buffer zero-copy return fastpath restricted to exact `type(data) is bytes`.
     - `PyBuffer_Release` in `finally` inside `_do_memmove` and `close()` draining pending executor writes (`_done_event.wait()`) before `_result_bytes` can be dropped.
     - Short-read check in `get_value` (`_total_bytes_written < expected_size`) so uninitialized heap bytes are never returned to callers.
     - Never bind `argtypes`/`restype` on the shared `ctypes.pythonapi`.
4. **Legacy token cache & bare `except:` (`gcsfs/credentials.py`)**:
   - `GoogleCredentials.load_tokens` (called at `import gcsfs` in `gcsfs/core.py`) / `_save_tokens` uses `pickle` on `~/.gcs_tokens` (`tfile`), and `_dict_to_credentials` / `_connect_token` contain legacy bare `except:` blocks.
   - **Traps**: Never expand `pickle` usage to untrusted paths; ensure any touched credential file creation uses `0o600` permissions; narrow bare `except:` to specific `google.auth` / `ValueError` / `json.JSONDecodeError` exceptions so unexpected errors don't silently fall through to wrong auth modes.
5. **Rapid / Zonal & RCU eventual-consistency `size` (`gcsfs/zonal_file.py`, `gcsfs/extended_gcsfs.py`)**:
   - Metadata `size` on Zonal (`BucketLayout.is_zonal`) and RCU (`BucketLayout.is_rcu`) buckets can be stale while appends are open. Using stale `size` to size raw buffers without clamping actual bytes returned can cause buffer mismatches or silent truncation.
6. **Resource & retry exhaustion (`gcsfs/poller.py`, `gcsfs/retry.py`, `gcsfs/core.py`)**:
   - Regressing the existing capped backoff (`retry.py`, `poller.py` `_TRANSIENT_ERROR_BACKOFF_CAP`), adding uncapped `Retry-After` handling, or unbounding read/prefetch buffers (`MAX_PREFETCH_SIZE`, `max_prefetch_size`) enables memory/CPU/control-plane DoS on adversarial or throttled buckets.
7. **CMEK (`kms_key_name`) must fail closed (`extended_gcsfs.py` `_validate_grpc_write_kms_key_name`, `zonal_file.py` `_resolve_kms_append`)**:
   - Unlike `metadata`/`content_type`, which gRPC writes may ignore with a warning, an unsupported or mismatched `kms_key_name` must raise rather than silently write with default encryption.

---

## 3. Targeted Discovery Commands (Run in Sandbox)

Use these fast `git grep` scans when auditing `gcsfs`:

```bash
# 1. Find REST URL strings built by f-string / `.format` / `+` interpolation; confirm every bucket/object segment goes through `quote()` or `_call(..., *args)`
git grep -n -E 'f["'\''](\{[^}]*location\}|/?(b|o|upload|download|storage)/)|/storage/v1/b/\{[^}]*\}|"b/" \+' -- gcsfs/ ':!gcsfs/tests/'

# 2. Check CPython C-API bindings, buffer-protocol pairing, and raw memmove sites
git grep -n -E 'ctypes|PyDLL|pythonapi|PyBytes_|PyObject_GetBuffer|PyBuffer_Release|memmove|from_address' -- gcsfs/ ':!gcsfs/tests/'

# 3. Audit credential, header, and token logging / serialization surfaces
git grep -n -i -E 'authorization|bearer|encryption-key|client_secret|refresh_token|access_token|__getstate__|__setstate__|__reduce__|pickle\.' -- gcsfs/ ':!gcsfs/tests/'

# 4. Check TLS settings, plaintext endpoints, and HTTP client calls (then verify `timeout=` by reading each call)
git grep -n -E 'ssl\.|ssl[[:space:]]*=|verify[[:space:]]*=|CERT_NONE|check_hostname|http://|(requests|[sS]ession)\.(get|post|put|delete|head|request)\(' -- gcsfs/ ':!gcsfs/tests/' .github/ cloudbuild/

# 5. Check subprocess, YAML/eval/exec, and tempfile usage across repo & CI scripts
git grep -n -E 'subprocess|shell[[:space:]]*=|yaml\.load|eval\(|exec\(|mktemp|Temporary' -- gcsfs/ .github/ cloudbuild/

# 6. Audit GitHub Actions and pre-commit pins, permissions, and `${{ ... }}` in `run:` blocks
git grep -n -E 'uses:|permissions:|pull_request_target|workflow_run|\$\{\{' -- .github/workflows/
git grep -n -E 'rev:' -- .pre-commit-config.yaml
```
