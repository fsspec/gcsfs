import pathlib
from types import SimpleNamespace

from gcsfs.tests.perf.subsystembenchmarks._common import limitations


def test_limitation_reason_applies_predicate(monkeypatch):
    entry = limitations.Limitation(
        group="checkpointing/ray_pytorch",
        accelerator="tpu",
        applies=lambda p: p.strategy == "single",
        reason="single unsupported; see gcsfs/tests/perf/subsystembenchmarks/limitations.md#ray-single",
    )
    monkeypatch.setattr(limitations, "KNOWN", (entry,))

    assert (
        limitations.limitation_reason(
            "checkpointing/ray_pytorch", SimpleNamespace(strategy="single"), "tpu"
        )
        == entry.reason
    )
    assert (
        limitations.limitation_reason(
            "checkpointing/ray_pytorch", SimpleNamespace(strategy="fsdp_sharded"), "tpu"
        )
        is None
    )
    assert (
        limitations.limitation_reason(
            "checkpointing/ray_pytorch", SimpleNamespace(strategy="single"), "gpu"
        )
        is None
    )
    assert (
        limitations.limitation_reason(
            "checkpointing/pytorch_save", SimpleNamespace(strategy="single"), "tpu"
        )
        is None
    )


def test_known_entries_cite_repro():
    limitations_doc = pathlib.Path(__file__).resolve().parents[1] / "limitations.md"
    assert limitations_doc.is_file()
    doc_text = limitations_doc.read_text()
    for entry in limitations.KNOWN:
        assert "limitations.md#" in entry.reason
        anchor = entry.reason.split("limitations.md#", 1)[1].split()[0].rstrip(".,);")
        assert f"#{anchor}" in doc_text or f'id="{anchor}"' in doc_text


def test_lightning_tpu_limitation_registered():
    reason = limitations.limitation_reason(
        "checkpointing/pytorch_lightning", SimpleNamespace(strategy="ddp"), "tpu"
    )
    assert reason is not None
    assert "limitations.md#lightning-tpu" in reason
    assert (
        limitations.limitation_reason(
            "checkpointing/pytorch_lightning", SimpleNamespace(strategy="ddp"), "gpu"
        )
        is None
    )
