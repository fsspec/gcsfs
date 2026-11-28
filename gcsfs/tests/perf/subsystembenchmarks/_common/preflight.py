"""Pre-run permission and resource checks for benchmark pods and VMs."""

from __future__ import annotations

import uuid

from gcsfs.tests.perf.subsystembenchmarks._common import config_loader


class PreflightError(RuntimeError):
    """Raised when pre-run GCS bucket or model read checks fail."""


def config_model_ids(group_dir: str, *, model_id_override: str = "") -> set[str]:
    """Return ``gs://`` model IDs referenced by ``group_dir/configs.yaml``.

    When ``model_id_override`` is non-empty, ``OneFactorCheckpointConfigurator``
    skips variants that pin ``model_id`` and runs all remaining cases with the
    override, so only ``model_id_override`` (if ``gs://``) is checked.
    """
    if model_id_override:
        return {model_id_override} if model_id_override.startswith("gs://") else set()

    data = config_loader.load_group_config(group_dir)
    if not data:
        return set()

    ids: set[str] = set()
    common_mid = (data.get("common") or {}).get("model_id", "")
    if isinstance(common_mid, str) and common_mid.startswith("gs://"):
        ids.add(common_mid)

    for scenario in data.get("scenarios") or []:
        if not isinstance(scenario, dict):
            continue
        scen_mid = scenario.get("model_id", "")
        if isinstance(scen_mid, str) and scen_mid.startswith("gs://"):
            ids.add(scen_mid)
        for variant in scenario.get("variants") or []:
            if not isinstance(variant, dict):
                continue
            if variant.get("enabled") is False:
                continue
            var_mid = variant.get("model_id", "")
            if isinstance(var_mid, str) and var_mid.startswith("gs://"):
                ids.add(var_mid)

    return ids


def _preflight_bucket_name(bucket_prefix: str) -> str:
    suffix = f"-pf-{uuid.uuid4().hex[:8]}"
    prefix = bucket_prefix.lower().strip("-")
    max_prefix = 63 - len(suffix)
    return f"{prefix[:max_prefix].rstrip('-')}{suffix}"


def check(
    project: str,
    location: str,
    bucket_prefix: str,
    model_ids: set[str],
    *,
    check_bucket: bool = True,
    bucket_kwargs: dict | None = None,
    fs=None,
) -> None:
    """Verify the GCS operations a run needs before running any case.

    Creates and deletes a bucket with ``bucket_kwargs`` (the run's bucket tier, from
    ``dataloading.bucket.bucket_kwargs``), and for each ``gs://`` model lists its files
    and reads one byte, as model staging does.
    """
    gs_models = sorted(m for m in model_ids if m.startswith("gs://"))
    if not check_bucket and not gs_models:
        return
    if fs is None:
        import gcsfs

        fs = gcsfs.GCSFileSystem(project=project)

    if check_bucket:
        bucket_name = _preflight_bucket_name(bucket_prefix)
        try:
            fs.mkdir(bucket_name, location=location, **(bucket_kwargs or {}))
        except Exception as exc:
            raise PreflightError(
                f"failed to create preflight bucket gs://{bucket_name} "
                f"in project {project!r} ({location}): {exc}"
            ) from exc
        try:
            fs.rmdir(bucket_name)
        except Exception as exc:
            raise PreflightError(
                f"failed to delete preflight bucket gs://{bucket_name}: {exc}"
            ) from exc

    for model_id in gs_models:
        _check_model_readable(fs, model_id)


def _check_model_readable(fs, model_id: str) -> None:
    try:
        entries = fs.ls(model_id, detail=True)
    except Exception as exc:
        raise PreflightError(f"failed to list model {model_id}: {exc}") from exc
    files = [
        e["name"]
        for e in entries
        if e.get("type") == "file" and not e["name"].endswith("/")
    ]
    if not files:
        raise PreflightError(
            f"model path {model_id} has no files (empty, only a folder placeholder, "
            "or inaccessible)"
        )
    sample = next((f for f in files if f.rsplit("/", 1)[-1] == "config.json"), files[0])
    try:
        fs.cat_file(sample, start=0, end=1)
    except Exception as exc:
        raise PreflightError(
            f"failed to read model object gs://{sample}: {exc}"
        ) from exc
