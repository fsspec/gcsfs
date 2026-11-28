import os
import types

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common import (
    accelerator,
    benchmark_publish,
    cli,
    limitations,
    placement,
)


def _case(name="c", ws=8, *, multi_node=False, gpu_share=False):
    return types.SimpleNamespace(
        name=name,
        max_world_size=lambda: ws,
        strategy="fsdp",
        MULTI_NODE=multi_node,
        GPU_RANKS_SHARE_DEVICES=gpu_share,
    )


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    accelerator.reset_cache()
    for key in (
        accelerator.DEVICE_ENV,
        placement.NODES_ENV,
        placement.DEVICES_PER_NODE_ENV,
        placement.COLLECTED_CASES_FILE_ENV,
        placement.GROUP_ENV,
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    accelerator.reset_cache()


@pytest.fixture
def on_cpu(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("cpu", 0, "none"))


@pytest.fixture
def on_cuda_8(monkeypatch):
    monkeypatch.setattr(
        accelerator, "_probe_accelerator", lambda: ("cuda", 8, "NVIDIA H200")
    )


@pytest.fixture
def on_cuda_4(monkeypatch):
    monkeypatch.setattr(
        accelerator, "_probe_accelerator", lambda: ("cuda", 4, "NVIDIA H200")
    )


@pytest.fixture
def on_tpu_4(monkeypatch):
    monkeypatch.setattr(accelerator, "_probe_accelerator", lambda: ("tpu", 4, "v6e"))


def test_nodes_needed():
    assert [placement.nodes_needed(w, 8) for w in (1, 8, 9, 16, 17)] == [
        1,
        1,
        2,
        2,
        3,
    ]
    with pytest.raises(ValueError):
        placement.nodes_needed(0, 8)
    with pytest.raises(ValueError):
        placement.nodes_needed(8, 0)


def test_skip_wider_than_host_on_accelerator(on_cuda_8):
    assert (
        placement.skip_reason(_case(ws=16), "checkpointing/pytorch_save")
        == "needs 16 devices; host has 8"
    )
    assert placement.skip_reason(_case(ws=8), "checkpointing/pytorch_save") is None


def test_no_width_skip_on_cpu_up_to_16(on_cpu):
    assert placement.skip_reason(_case(ws=16), "checkpointing/pytorch_save") is None
    assert (
        placement.skip_reason(_case(ws=32), "checkpointing/pytorch_save")
        == "needs 32 devices; host has 16"
    )


def test_select_keeps_only_matching_nodes(on_cpu, monkeypatch):
    monkeypatch.setenv(placement.NODES_ENV, "2")
    monkeypatch.setenv(placement.DEVICES_PER_NODE_ENV, "8")
    cases = [
        _case("w8", 8, multi_node=True),
        _case("w16a", 16, multi_node=True),
        _case("w16b", 16, multi_node=True),
        _case("single16", 16),
    ]
    selected = placement.select(cases, group="checkpointing/pytorch_save")
    assert [p.id for p in selected] == ["w16a", "w16b"]
    assert all(not p.marks for p in selected)


def test_select_uses_devices_per_node_env_over_probe(on_cuda_4, monkeypatch):
    monkeypatch.setenv(placement.DEVICES_PER_NODE_ENV, "8")
    monkeypatch.setenv(placement.NODES_ENV, "1")
    monkeypatch.setattr(
        accelerator,
        "local_device_count",
        lambda: pytest.fail("should not probe local count"),
    )
    selected = placement.select([_case("w8", 8)], group="checkpointing/pytorch_save")
    assert [p.id for p in selected] == ["w8"]
    assert selected[0].marks == ()


def test_select_marks_limitations_as_skipped(on_cuda_8, monkeypatch):
    entry = limitations.Limitation(
        group="g",
        accelerator="gpu",
        applies=lambda p: p.name == "bad",
        reason="broken; see limitations.md#bad",
    )
    monkeypatch.setattr(limitations, "KNOWN", (entry,))
    selected = placement.select([_case("ok", 8), _case("bad", 8)], group="g")
    assert selected[0].marks == ()
    skip, limitation = selected[1].marks
    assert skip.mark.name == "skip"
    assert skip.mark.kwargs["reason"] == entry.reason
    assert limitation.mark.name == "limitation"


def test_collect_node_counts_dcp_group(monkeypatch):
    pytest.importorskip("torch")
    monkeypatch.setenv(placement.DEVICES_PER_NODE_ENV, "8")
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cpu")
    suite_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "checkpointing", "pytorch_save"
    )
    assert cli.collect_node_counts(suite_dir) == [1, 2]
    assert cli.collect_node_counts(suite_dir, filter_expr="pp-hsdp-tp") == [2]


def test_machine_info_records_nodes(on_cpu, monkeypatch):
    info = {}
    benchmark_publish.pytest_benchmark_update_machine_info(None, info)
    assert info["nodes"] == 1
    monkeypatch.setenv(placement.NODES_ENV, "2")
    info2 = {}
    benchmark_publish.pytest_benchmark_update_machine_info(None, info2)
    assert info2["nodes"] == 2


def test_skip_reason_during_node_count_listing_skips_limitations_not_width(
    on_cpu, monkeypatch
):
    entry = limitations.Limitation(
        group="checkpointing/pytorch_lightning",
        accelerator="tpu",
        applies=lambda p: True,
        reason="unsupported; see limitations.md#lightning-tpu",
    )
    monkeypatch.setattr(limitations, "KNOWN", (entry,))
    monkeypatch.setenv(placement.COLLECTED_CASES_FILE_ENV, "/tmp/unused.json")
    monkeypatch.setenv(placement.DEVICES_PER_NODE_ENV, "8")
    monkeypatch.setenv(accelerator.DEVICE_ENV, "tpu")
    assert placement.skip_reason(_case(ws=16), "checkpointing/pytorch_save") is None
    assert (
        placement.skip_reason(_case(ws=8), "checkpointing/pytorch_lightning")
        == entry.reason
    )


@pytest.mark.parametrize(
    "case, per_node, accel, expected",
    [
        (_case(ws=8), 8, "gpu", 1),
        (_case(ws=16), 8, "gpu", None),
        (_case(ws=16, multi_node=True), 8, "gpu", 2),
        (_case(ws=16, multi_node=True), 8, "tpu", 2),
        (_case(ws=8, gpu_share=True), 1, "gpu", 1),
        (_case(ws=8, gpu_share=True), 4, "tpu", None),
        (_case(ws=8, gpu_share=True), 4, "cpu", None),
        (_case(ws=8, gpu_share=True), 16, "cpu", 1),
    ],
)
def test_case_nodes(case, per_node, accel, expected):
    assert placement.case_nodes(case, per_node, accel) == expected


def test_gpu_dataloading_ranks_share_devices(on_cuda_4):
    shared = _case(ws=8, gpu_share=True)
    assert placement.skip_reason(shared, "dataloading/huggingface_datasets") is None
    assert (
        placement.skip_reason(_case(ws=8), "checkpointing/ray_pytorch")
        == "needs 8 devices; host has 4"
    )


def test_tpu_dataloading_ranks_stay_exclusive(on_tpu_4):
    assert (
        placement.skip_reason(_case(ws=8, gpu_share=True), "dataloading/webdataset")
        == "needs 8 devices; host has 4"
    )
    assert (
        placement.skip_reason(_case(ws=4, gpu_share=True), "dataloading/webdataset")
        is None
    )


def test_select_multi_node_job_drops_single_node_cases(on_cpu, monkeypatch):
    monkeypatch.setenv(placement.NODES_ENV, "2")
    monkeypatch.setenv(placement.DEVICES_PER_NODE_ENV, "4")
    cases = [_case("wds8", 8, gpu_share=True), _case("ray8", 8)]
    assert placement.select(cases, group="dataloading/webdataset") == []


def test_select_limited_group_does_not_probe_devices_when_listing(on_cpu, monkeypatch):
    entry = limitations.Limitation(
        group="checkpointing/pytorch_lightning",
        accelerator="tpu",
        applies=lambda p: True,
        reason="unsupported; see limitations.md#lightning-tpu",
    )
    monkeypatch.setattr(limitations, "KNOWN", (entry,))
    monkeypatch.setenv(placement.COLLECTED_CASES_FILE_ENV, "/tmp/unused.json")
    monkeypatch.setenv(accelerator.DEVICE_ENV, "tpu")
    monkeypatch.setattr(
        accelerator, "local_device_count", lambda: pytest.fail("should not probe")
    )
    (param,) = placement.select(
        [_case("pl", 8)], group="checkpointing/pytorch_lightning"
    )
    assert param.marks[0].mark.kwargs["reason"] == entry.reason


def test_list_node_counts_keeps_single_node_groups_on_one_node(monkeypatch):
    pytest.importorskip("webdataset")
    pytest.importorskip("ray")
    monkeypatch.setenv(placement.DEVICES_PER_NODE_ENV, "4")
    monkeypatch.setenv(accelerator.DEVICE_ENV, "cuda")
    root = os.path.dirname(os.path.dirname(__file__))
    for group in ("dataloading/webdataset", "checkpointing/ray_pytorch"):
        assert cli.collect_node_counts(os.path.join(root, group)) == [1], group
    save_dir = os.path.join(root, "checkpointing", "pytorch_save")
    assert cli.collect_node_counts(save_dir) == [2, 4]


def test_list_cases_without_devices_per_node_skips_probe_for_limited_group(monkeypatch):
    pytest.importorskip("lightning")
    monkeypatch.setenv(accelerator.DEVICE_ENV, "tpu")
    pl_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "checkpointing",
        "pytorch_lightning",
    )
    assert cli.collect_cases(pl_dir) == []
