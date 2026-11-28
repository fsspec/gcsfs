import os
import signal
import socket
import subprocess
import sys
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import (
    _tpu_torchrun,
    accelerator,
    launcher,
    nodes,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    accelerator.device_kind.cache_clear()
    launcher.reset_rdzv_step()
    for key in (
        accelerator.DEVICE_ENV,
        "TORCH_DEVICE_BACKEND_AUTOLOAD",
        "TORCH_TPU_TOPOLOGY",
        "TORCH_TPU_SLICEBUILDER_ADDRESSES",
        launcher.TPU_SLICE_TOPOLOGY_ENV,
        launcher.TPU_SLICE_ADDRESSES_ENV,
        "GCSFS_SUBSYSTEM_PORT_BASE",
        nodes.NODES_ENV,
        nodes.NODE_RANK_ENV,
        nodes.LEADER_ADDR_ENV,
        nodes.RDZV_PORT_ENV,
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    accelerator.device_kind.cache_clear()
    launcher.reset_rdzv_step()


@pytest.fixture
def on_cpu(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("cpu", 0, "none"))


@pytest.fixture
def on_cuda_8(monkeypatch):
    monkeypatch.setattr(
        accelerator, "_probe_accelerator", lambda: ("cuda", 8, "NVIDIA H200")
    )


@pytest.fixture
def on_tpu_4(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("tpu", 4, "v6e"))


@pytest.fixture
def capture_popen(monkeypatch):
    calls: list[dict] = []

    class FakePopen:
        def __init__(self, cmd, *, env=None, start_new_session=False):
            self.pid = 42424
            self.cmd = list(cmd)
            self.env = dict(env) if env is not None else {}
            self.start_new_session = start_new_session
            calls.append(
                {
                    "cmd": self.cmd,
                    "env": self.env,
                    "start_new_session": start_new_session,
                }
            )

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(subprocess, "Popen", FakePopen)
    return calls


def test_run_torchrun_single_node_standalone_command(
    on_cpu, capture_popen, monkeypatch
):
    monkeypatch.setattr(launcher, "find_free_port", lambda: 29511)
    ran = launcher.run_torchrun(
        "pkg.worker",
        ["--foo", "bar"],
        world_size=4,
        rdzv_id="case-1",
    )
    assert ran is True
    assert len(capture_popen) == 1
    call = capture_popen[0]
    assert call["start_new_session"] is True
    assert call["cmd"] == [
        sys.executable,
        "-m",
        "torch.distributed.run",
        "--standalone",
        "--nproc_per_node=4",
        "--master_port=29511",
        "-m",
        "pkg.worker",
        "--foo",
        "bar",
    ]
    assert call["env"][accelerator.DEVICE_ENV] == "cpu"


def test_run_torchrun_multinode_c10d_increments_port_and_sets_timeouts(
    on_cuda_8, capture_popen
):
    group = nodes.NodeGroup(
        node_rank=0, num_nodes=2, leader_addr="leader", rdzv_port=29400
    )

    assert (
        launcher.run_torchrun(
            "pkg.worker",
            ["--step", "1"],
            world_size=16,
            rdzv_id="case-1",
            node_group=group,
            devices_per_node=8,
        )
        is True
    )
    assert (
        launcher.run_torchrun(
            "pkg.worker",
            ["--step", "2"],
            world_size=16,
            rdzv_id="case-2",
            node_group=group,
            devices_per_node=8,
        )
        is True
    )

    assert len(capture_popen) == 2
    cmd0, cmd1 = capture_popen[0]["cmd"], capture_popen[1]["cmd"]
    assert cmd0 == [
        sys.executable,
        "-m",
        "torch.distributed.run",
        "--nnodes=2",
        "--nproc_per_node=8",
        "--node_rank=0",
        "--rdzv_backend=c10d",
        "--rdzv_endpoint=leader:29400",
        "--rdzv_id=case-1",
        "--rdzv_conf=read_timeout=1800,join_timeout=7200",
        "-m",
        "pkg.worker",
        "--step",
        "1",
    ]
    assert "--rdzv_endpoint=leader:29401" in cmd1
    assert "--rdzv_id=case-2" in cmd1


def test_asymmetric_single_node_phase_does_not_advance_multinode_rdzv_port(
    on_cuda_8, capture_popen
):
    # Simulate Pod 0
    pod0 = nodes.NodeGroup(
        node_rank=0, num_nodes=2, leader_addr="leader", rdzv_port=29400
    )
    launcher.reset_rdzv_step()
    assert (
        launcher.run_torchrun(
            "pkg.worker",
            ["--phase", "single"],
            world_size=8,
            rdzv_id="single",
            node_group=pod0,
            devices_per_node=8,
        )
        is True
    )
    assert "--standalone" in capture_popen[-1]["cmd"]

    assert (
        launcher.run_torchrun(
            "pkg.worker",
            ["--phase", "multi"],
            world_size=16,
            rdzv_id="multi",
            node_group=pod0,
            devices_per_node=8,
        )
        is True
    )
    pod0_multi_cmd = capture_popen[-1]["cmd"]

    # Simulate Pod 1
    pod1 = nodes.NodeGroup(
        node_rank=1, num_nodes=2, leader_addr="leader", rdzv_port=29400
    )
    launcher.reset_rdzv_step()
    before_pod1 = len(capture_popen)
    assert (
        launcher.run_torchrun(
            "pkg.worker",
            ["--phase", "single"],
            world_size=8,
            rdzv_id="single",
            node_group=pod1,
            devices_per_node=8,
        )
        is False
    )
    assert len(capture_popen) == before_pod1

    assert (
        launcher.run_torchrun(
            "pkg.worker",
            ["--phase", "multi"],
            world_size=16,
            rdzv_id="multi",
            node_group=pod1,
            devices_per_node=8,
        )
        is True
    )
    pod1_multi_cmd = capture_popen[-1]["cmd"]

    assert "--rdzv_endpoint=leader:29400" in pod0_multi_cmd
    assert "--rdzv_endpoint=leader:29400" in pod1_multi_cmd


def test_partial_subset_multinode_phase_keeps_rdzv_port_in_lockstep(
    on_cuda_8, capture_popen
):
    endpoints_for_4node: dict[int, str] = {}
    for rank in range(4):
        group = nodes.NodeGroup(
            node_rank=rank, num_nodes=4, leader_addr="leader", rdzv_port=29400
        )
        launcher.reset_rdzv_step()

        # Phase 1: 2-node subset (world_size=16 on 4x8 cluster)
        ran_subset = launcher.run_torchrun(
            "pkg.worker",
            ["--phase", "subset"],
            world_size=16,
            rdzv_id="subset-16",
            node_group=group,
            devices_per_node=8,
        )
        assert ran_subset is (rank < 2)
        if ran_subset:
            assert "--rdzv_endpoint=leader:29400" in capture_popen[-1]["cmd"]

        # Phase 2: full 4-node launch (world_size=32)
        ran_full = launcher.run_torchrun(
            "pkg.worker",
            ["--phase", "full"],
            world_size=32,
            rdzv_id="full-32",
            node_group=group,
            devices_per_node=8,
        )
        assert ran_full is True
        endpoint_arg = next(
            arg
            for arg in capture_popen[-1]["cmd"]
            if arg.startswith("--rdzv_endpoint=")
        )
        endpoints_for_4node[rank] = endpoint_arg

    assert endpoints_for_4node == {
        0: "--rdzv_endpoint=leader:29401",
        1: "--rdzv_endpoint=leader:29401",
        2: "--rdzv_endpoint=leader:29401",
        3: "--rdzv_endpoint=leader:29401",
    }


def test_run_torchrun_1node_tpu_uses_tpu_torchrun_wrapper_when_world_size_gt_1(
    on_tpu_4, capture_popen, monkeypatch
):
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "stale-topo")
    monkeypatch.setenv("TORCH_TPU_SLICEBUILDER_ADDRESSES", "stale-addr")
    monkeypatch.setattr(launcher, "find_free_port", lambda: 29520)

    launcher.run_torchrun("pkg.worker", ["--a"], world_size=4, rdzv_id="tpu-4")
    call_4 = capture_popen[-1]
    assert call_4["cmd"][:6] == [
        sys.executable,
        "-m",
        "gcsfs.tests.perf.subsystembenchmarks._common._tpu_torchrun",
        "--nproc-per-node",
        "4",
        "--",
    ]
    assert "--standalone" in call_4["cmd"][6:]
    assert "TORCH_TPU_TOPOLOGY" not in call_4["env"]
    assert "TORCH_TPU_SLICEBUILDER_ADDRESSES" not in call_4["env"]

    launcher.run_torchrun("pkg.worker", ["--b"], world_size=1, rdzv_id="tpu-1")
    call_1 = capture_popen[-1]
    assert call_1["cmd"][:3] == [sys.executable, "-m", "torch.distributed.run"]
    assert "TORCH_TPU_TOPOLOGY" not in call_1["env"]
    assert "TORCH_TPU_SLICEBUILDER_ADDRESSES" not in call_1["env"]

    launcher.run_torchrun(
        "pkg.worker",
        ["--c"],
        world_size=4,
        rdzv_id="tpu-noprep",
        prepare_tpu_env=False,
    )
    call_noprep = capture_popen[-1]
    assert call_noprep["cmd"][:3] == [sys.executable, "-m", "torch.distributed.run"]
    assert "TORCH_TPU_TOPOLOGY" not in call_noprep["env"]
    assert "TORCH_TPU_SLICEBUILDER_ADDRESSES" not in call_noprep["env"]


def test_run_torchrun_multinode_tpu_forwards_slice_env_and_fails_if_missing(
    on_tpu_4, capture_popen, monkeypatch
):
    group = nodes.NodeGroup(
        node_rank=0, num_nodes=2, leader_addr="leader", rdzv_port=29400
    )

    with pytest.raises(accelerator.AcceleratorSetupError):
        launcher.run_torchrun(
            "pkg.worker",
            [],
            world_size=8,
            rdzv_id="tpu-multi-missing",
            node_group=group,
            devices_per_node=4,
        )

    monkeypatch.setenv(launcher.TPU_SLICE_TOPOLOGY_ENV, "2x4")
    monkeypatch.setenv(launcher.TPU_SLICE_ADDRESSES_ENV, "10.0.0.1:8476,10.0.0.2:8476")
    launcher.reset_rdzv_step()
    launcher.run_torchrun(
        "pkg.worker",
        [],
        world_size=8,
        rdzv_id="tpu-multi-ok",
        node_group=group,
        devices_per_node=4,
    )
    call = capture_popen[-1]
    assert call["cmd"][:3] == [sys.executable, "-m", "torch.distributed.run"]
    assert call["env"]["TORCH_TPU_TOPOLOGY"] == "2x4"
    assert call["env"]["TORCH_TPU_SLICEBUILDER_ADDRESSES"] == (
        "10.0.0.1:8476,10.0.0.2:8476"
    )


def test_launcher_strips_inherited_torch_device_backend_autoload(
    on_cpu, capture_popen, monkeypatch
):
    monkeypatch.setenv("TORCH_DEVICE_BACKEND_AUTOLOAD", "0")

    launcher.run_torchrun("pkg.worker", [], world_size=2, rdzv_id="strip-1")
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in capture_popen[-1]["env"]

    launcher.run_subprocess("pkg.setup", ["--flag"])
    assert "TORCH_DEVICE_BACKEND_AUTOLOAD" not in capture_popen[-1]["env"]
    assert capture_popen[-1]["cmd"] == [sys.executable, "-m", "pkg.setup", "--flag"]
    assert capture_popen[-1]["env"][accelerator.DEVICE_ENV] == "cpu"


def test_launcher_terminates_process_group_on_keyboard_interrupt(on_cpu, monkeypatch):
    signals_sent: list[tuple[int, int]] = []
    wait_calls: list[float | None] = []

    class HangingPopen:
        def __init__(self, cmd, *, env=None, start_new_session=False):
            assert start_new_session is True
            self.pid = 55555

        def wait(self, timeout=None):
            wait_calls.append(timeout)
            if len(wait_calls) == 1:
                raise KeyboardInterrupt
            if len(wait_calls) == 2:
                raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout)
            return -9

    monkeypatch.setattr(subprocess, "Popen", HangingPopen)
    monkeypatch.setattr(os, "killpg", lambda pid, sig: signals_sent.append((pid, sig)))

    with pytest.raises(KeyboardInterrupt):
        launcher.run_subprocess("pkg.worker", [])

    assert signals_sent == [(55555, signal.SIGTERM), (55555, signal.SIGKILL)]
    assert wait_calls == [None, 10, None]


def test_find_free_port_uses_disjoint_port_base_block(monkeypatch):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)
        base = int(occupied.getsockname()[1])
        monkeypatch.setenv("GCSFS_SUBSYSTEM_PORT_BASE", str(base))
        chosen = launcher.find_free_port()
        assert base < chosen < base + launcher.PORT_BLOCK_SIZE


def test_tpu_torchrun_strips_env_sets_launch_env_and_execs(monkeypatch):
    monkeypatch.setenv("TORCH_TPU_TOPOLOGY", "old")
    monkeypatch.setenv("TORCH_TPU_SLICEBUILDER_ADDRESSES", "old")

    recorded: dict = {}

    def fake_set_tpu_launch_env(*, nproc_per_node):
        recorded["nproc_per_node"] = nproc_per_node
        recorded["env_during_call"] = {
            "topo": os.environ.get("TORCH_TPU_TOPOLOGY"),
            "addr": os.environ.get("TORCH_TPU_SLICEBUILDER_ADDRESSES"),
        }

    fake_dist = types.SimpleNamespace(set_tpu_launch_env=fake_set_tpu_launch_env)
    fake_tpu = types.SimpleNamespace(distributed=fake_dist)
    fake_torch = types.SimpleNamespace(tpu=fake_tpu)
    monkeypatch.setitem(sys.modules, "torch", fake_torch)
    monkeypatch.setitem(sys.modules, "torch.tpu", fake_tpu)
    monkeypatch.setitem(sys.modules, "torch.tpu.distributed", fake_dist)

    def fake_execv(executable, argv):
        recorded["execv"] = (executable, list(argv))

    monkeypatch.setattr(os, "execv", fake_execv)

    _tpu_torchrun.main(
        ["--nproc-per-node", "4", "--", "--standalone", "--nproc_per_node=4", "-m", "m"]
    )

    assert recorded["nproc_per_node"] == 4
    assert recorded["env_during_call"] == {"topo": None, "addr": None}
    assert recorded["execv"] == (
        sys.executable,
        [
            sys.executable,
            "-m",
            "torch.distributed.run",
            "--standalone",
            "--nproc_per_node=4",
            "-m",
            "m",
        ],
    )
