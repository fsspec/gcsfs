---
trigger: always_on
description: "Mandatory security boundaries and secure-by-default coding rules for gcsfs (credentials, REST/local paths, TLS/mTLS, CPython ctypes buffers, deserialization, and CI workflows)."
---

# GCSFS Security Guardrails

Apply these three-tier boundaries on every change in `gcsfs` (runtime SDK, tests, `cloudbuild/`, and `.github/`).

## 1. Always Do (Non-Negotiable)

- **Quote every REST path segment**: Pass `bucket`, `key`, and folder names via positional `*args` to `_call` / `_request` / `_format_path` (e.g., `self._call("GET", "b/{}/o/{}", bucket, key)`) or explicit `gcsfs.core.quote(val)` (`safe=""`). Never interpolate unquoted bucket or object names in URL f-strings, where `/`, `?`, `#`, or `..` can alter endpoints or inject query parameters.
- **Guard local filesystem writes**: In `get`, `_get_file`, cache writers, and unpack/archive paths, ensure remote object keys containing `..`, leading `/`, drive prefixes, or symlinks cannot escape the caller's target local directory.
- **Redact credentials & encryption keys everywhere**: Never include `Authorization`, `Proxy-Authorization`, `X-Goog-Encryption-Key*` (CSEK), raw/refresh tokens, client secrets, or signed URL query strings in `logger.*` (including `DEBUG`; note `_call` logs `kwargs.get("headers")`), exception messages, `repr()`, telemetry attributes (`gcsfs/telemetry/`), or pickled state (`__getstate__` / `__reduce__`).
- **Protect local credential & cert files**: Write any local token cache or temporary key/certificate material using `0o600` file permissions (`os.open(..., os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)`) or inside a `tempfile.TemporaryDirectory()` (`0o700`), and clean up temporary key files promptly.
- **Fail closed on TLS, mTLS & endpoints**: Use `ssl.create_default_context()` with certificate and hostname verification enabled. Restrict plaintext `http://` to explicit `STORAGE_EMULATOR_HOST` or caller-supplied `endpoint_url`, and never forward auth headers across `https://` -> `http://` or untrusted cross-host redirects.
- **Enforce bounds & lifetimes on `ctypes` / zero-copy buffers**: Before any `ctypes.memmove` or pointer arithmetic (`zb_hns_utils.py`, CPython `PyBytes_*`), check `NULL` returns from `ctypes.pythonapi`, validate `0 <= offset`, `0 <= size`, and `offset + size <= total_size` in Python, keep the owning `bytes` object alive until all worker futures/tasks finish (`wait` / `gather`), and never return uninitialized `PyBytes_FromStringAndSize(None, n)` memory on short reads or errors.
- **Harden subprocesses, HTTP calls & CI**: Pass argument lists with `shell=False` to `subprocess`, pass explicit `timeout=` to `requests` / `aiohttp` calls, use `yaml.safe_load`, pin GitHub Actions to full 40-character commit SHAs with minimal `permissions: contents: read`, pass `${{ github.event.* }}` values via `env:` (never inline in `run:` scripts), and verify SHA-256 checksums on downloaded binaries.

## 2. Ask Human Before Proceeding

- Adding new runtime/optional dependencies or changing security-sensitive version floors in `pyproject.toml` or `environment_gcsfs.yaml`.
- Changing authentication fallback order, token persistence/serialization (`gcsfs/credentials.py`), universe-domain (`TPC`) / mTLS trust logic, or `anon` fallback semantics.
- Introducing new `ctypes.pythonapi` calls, raw pointer operations, `pickle` deserialization paths, or elevating `.github/workflows/` / `cloudbuild/` permissions.

## 3. Never Do

- Never commit real credentials, service-account JSON keys, access/refresh tokens, or private internal bucket/project identifiers (use `gcsfs/tests/fake-service-account-credentials.json` or fixtures/mocks in tests).
- Never call `pickle.loads`, `eval`, `exec`, or `yaml.load` (without `SafeLoader`) on GCS object contents, HTTP/gRPC responses, or untrusted files.
- Never silently disable SSL/TLS verification (`ssl=False`, `verify=False`, `CERT_NONE`) or silently fall back to unauthenticated/insecure transport on error.
