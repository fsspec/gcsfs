import dataclasses
import json
import subprocess
import sys

import pytest

pytest.importorskip("transformers")

from gcsfs.tests.perf.subsystembenchmarks._common import launcher, nodes  # noqa: E402
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.driver import (  # noqa: E402
    ResourceSnapshot,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch import (  # noqa: E402
    _worker,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.read.driver import (  # noqa: E402
    PyTorchCheckpointReadDriver,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.tests.tiny import (  # noqa: E402
    make_params,
)
from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.write.driver import (  # noqa: E402
    PyTorchCheckpointWriteDriver,
)

WORKER_MODULE = "gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch._worker"


def _write_spec(path, prefix: str, params) -> str:
    spec_path = path / "spec.json"
    spec_path.write_text(
        json.dumps({"prefix": prefix, "params": dataclasses.asdict(params)})
    )
    return str(spec_path)


def _run_standalone_torchrun(op: str, spec_file: str, result_file: str | None = None):
    args = [
        sys.executable,
        "-m",
        "torch.distributed.run",
        "--standalone",
        "--nproc_per_node=2",
        f"--master_port={launcher.find_free_port()}",
        "-m",
        WORKER_MODULE,
        "--op",
        op,
        "--spec-file",
        spec_file,
    ]
    if result_file is not None:
        args.extend(["--result-file", result_file])
    proc = subprocess.run(args, capture_output=True, text=True, timeout=180)
    assert proc.returncode == 0, f"{op} failed:\n{proc.stdout}\n{proc.stderr}"


def test_worker_save_and_verify_saved_roundtrip(tmp_path, tiny_models):
    params = make_params(
        tiny_models["llama"],
        scenario="checkpoint_write",
        strategy="fsdp",
        dp=2,
        world_size=2,
        rounds=2,
    )
    ckpt_dir = tmp_path / "ckpt_save"
    prefix = ckpt_dir.as_uri()
    save_spec = _write_spec(tmp_path, prefix, params)
    result_file = tmp_path / "save_result.json"

    _run_standalone_torchrun("save", save_spec, str(result_file))

    assert result_file.is_file()
    data = json.loads(result_file.read_text())
    assert len(data["durations"]) == 2
    assert all(d > 0 for d in data["durations"])
    assert len(data["window"]) == 2
    assert data["window"][0] <= data["window"][1]
    rm = data["resource_metrics"]
    assert set(rm.keys()) == {
        "max_cpu",
        "max_mem",
        "net_recv",
        "net_sent",
        "duration",
        "vcpus",
    }
    assert rm["vcpus"] >= 1
    assert rm["duration"] >= 0
    assert rm["max_mem"] > 0
    assert len(data["stats"]) == 2
    assert all(s["bytes_written"] > 0 for s in data["stats"])

    verify_spec = _write_spec(
        tmp_path, f"{prefix.rstrip('/')}/step-{params.rounds - 1}/", params
    )
    _run_standalone_torchrun("verify_saved", verify_spec)


def test_worker_save_source_and_load_roundtrip(tmp_path, tiny_models):
    params = make_params(
        tiny_models["llama"],
        scenario="checkpoint_read",
        strategy="fsdp",
        dp=2,
        world_size=2,
        rounds=2,
    )
    ckpt_dir = tmp_path / "ckpt_load"
    prefix = ckpt_dir.as_uri()
    spec = _write_spec(tmp_path, prefix, params)
    result_file = tmp_path / "load_result.json"

    _run_standalone_torchrun("save_source", spec)
    _run_standalone_torchrun("load", spec, str(result_file))

    assert result_file.is_file()
    data = json.loads(result_file.read_text())
    assert len(data["durations"]) == 2
    assert all(d > 0 for d in data["durations"])
    assert len(data["window"]) == 2
    assert data["window"][0] <= data["window"][1]
    rm = data["resource_metrics"]
    assert set(rm.keys()) == {
        "max_cpu",
        "max_mem",
        "net_recv",
        "net_sent",
        "duration",
        "vcpus",
    }
    assert rm["vcpus"] >= 1
    assert rm["duration"] >= 0
    assert rm["max_mem"] > 0
    assert len(data["stats"]) == 2
    assert all(s["bytes_fetched"] > 0 and s["read_items"] > 0 for s in data["stats"])


def test_reduce_gathered_normalizes_clocks_per_node():
    # Node 0 has monotonic epoch around 1000s; Node 1 has epoch around 5000s.
    gathered = [
        (0, [(1000.0, 1002.0), (1010.0, 1011.5)], {"bytes_written": 10}),
        (0, [(1000.2, 1002.5), (1010.1, 1011.0)], {"bytes_written": 20}),
        (1, [(5000.0, 5003.0), (5010.0, 5012.0)], {"bytes_written": 30}),
        (1, [(5000.5, 5002.8), (5010.2, 5011.8)], {"bytes_written": 40}),
    ]
    durations, stats = _worker._reduce_gathered(gathered, world_size=4, rounds=2)
    assert durations == pytest.approx([3.0, 2.0])
    assert stats == [
        {"bytes_written": 10},
        {"bytes_written": 20},
        {"bytes_written": 30},
        {"bytes_written": 40},
    ]


def test_write_driver_launches_save_and_verify_as_separate_torchrun_calls(
    tmp_path, monkeypatch
):
    params = make_params(
        "m",
        name="case-save",
        scenario="checkpoint_write",
        strategy="fsdp",
        dp=2,
        world_size=2,
        rounds=2,
    )
    calls = []

    def _fake_run_torchrun(module, args, *, world_size, rdzv_id, node_group=None, **kw):
        calls.append((module, list(args), world_size, rdzv_id, node_group))
        op = args[args.index("--op") + 1]
        if op == "save":
            result_path = args[args.index("--result-file") + 1]
            with open(result_path, "w") as f:
                json.dump(
                    {
                        "durations": [1.2, 1.5],
                        "window": [100.0, 103.0],
                        "resource_metrics": {
                            "max_cpu": 50.0,
                            "max_mem": 1024.0,
                            "net_recv": 10.0,
                            "net_sent": 20.0,
                            "duration": 3.0,
                            "vcpus": 8,
                        },
                        "stats": [{"bytes_written": 100}, {"bytes_written": 200}],
                    },
                    f,
                )
        return True

    monkeypatch.setattr(launcher, "run_torchrun", _fake_run_torchrun)

    driver = PyTorchCheckpointWriteDriver()
    prefix = (tmp_path / "ckpt").as_uri()
    result = driver.run(prefix, params)

    assert len(calls) == 1
    assert calls[0][0] == WORKER_MODULE
    assert (
        "--op" in calls[0][1] and calls[0][1][calls[0][1].index("--op") + 1] == "save"
    )
    assert calls[0][2] == 2
    assert calls[0][3] == "case-save-save"

    assert result.durations == [1.2, 1.5]
    assert result.extra_columns == {"dcp_write_bytes_per_rank_max": 200}
    assert result.window == (100.0, 103.0)
    assert result.resource_metrics == ResourceSnapshot(
        max_cpu=50.0,
        max_mem=1024.0,
        net_recv=10.0,
        net_sent=20.0,
        duration=3.0,
        vcpus=8,
    )

    result.verify()
    assert len(calls) == 2
    assert calls[1][1][calls[1][1].index("--op") + 1] == "verify_saved"
    assert calls[1][2] == 2
    assert calls[1][3] == "case-save-verify"


def test_multinode_follower_returns_empty_result_and_joins_verify(
    tmp_path, monkeypatch
):
    follower = nodes.NodeGroup(
        node_rank=1, num_nodes=2, leader_addr="10.0.0.1", rdzv_port=29400
    )
    monkeypatch.setattr(nodes.NodeGroup, "from_env", classmethod(lambda cls: follower))

    calls = []

    def _fake_run_torchrun(module, args, *, world_size, rdzv_id, node_group=None, **kw):
        calls.append((args[args.index("--op") + 1], world_size, rdzv_id, node_group))
        # Follower does NOT write result.json (global rank 0 is on leader pod).
        return True

    monkeypatch.setattr(launcher, "run_torchrun", _fake_run_torchrun)

    write_params = make_params(
        "m",
        name="case-w",
        scenario="checkpoint_write",
        strategy="fsdp",
        dp=4,
        world_size=4,
        rounds=2,
    )
    w_driver = PyTorchCheckpointWriteDriver()
    w_res = w_driver.run((tmp_path / "w").as_uri(), write_params)
    assert w_res.durations == []
    assert w_res.verify is not None
    w_res.verify()

    read_params = make_params(
        "m",
        name="case-r",
        scenario="checkpoint_read",
        strategy="fsdp",
        dp=4,
        world_size=4,
        rounds=1,
        source={"strategy": "fsdp", "data_parallel_size": 2, "world_size": 2},
    )
    r_driver = PyTorchCheckpointReadDriver()
    monkeypatch.setattr(r_driver, "prepare", lambda *a, **kw: None)
    r_driver.setup((tmp_path / "r").as_uri(), read_params)
    r_res = r_driver.run((tmp_path / "r").as_uri(), read_params)
    assert r_res.durations == []
    assert r_res.verify is None

    assert calls == [
        ("save", 4, "case-w-save", follower),
        ("verify_saved", 4, "case-w-verify", follower),
        ("save_source", 2, "case-r-setup", follower),
        ("load", 4, "case-r-load", follower),
    ]
