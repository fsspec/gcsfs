import logging
import sys
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import accelerator, ray_runtime


@pytest.fixture
def fake_ray(monkeypatch):
    state = {"initialized": False, "init_kwargs": None}

    def init(**kwargs):
        state["initialized"] = True
        state["init_kwargs"] = kwargs

    def shutdown():
        state["initialized"] = False

    module = types.SimpleNamespace(
        init=init, shutdown=shutdown, is_initialized=lambda: state["initialized"]
    )
    monkeypatch.setitem(sys.modules, "ray", module)
    monkeypatch.setattr(accelerator, "device_kind", lambda: "cpu")
    return state


def test_ensure_ray_initialized_starts_local_cluster_despite_ray_address(
    fake_ray, monkeypatch
):
    monkeypatch.setenv("RAY_ADDRESS", "ray://10.0.0.1:10001")
    ray_runtime.ensure_ray_initialized()
    assert fake_ray["init_kwargs"] == {
        "address": "local",
        "include_dashboard": False,
        "ignore_reinit_error": True,
        "logging_level": logging.WARNING,
    }
    ray_runtime.shutdown_ray()
    assert fake_ray["initialized"] is False
