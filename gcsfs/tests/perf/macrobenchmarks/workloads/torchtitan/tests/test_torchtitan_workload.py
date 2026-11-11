"""Tests for the TorchTitan ``main`` gcsfs macrobenchmark recipe.

TorchTitan ``main`` needs a PyTorch nightly and its ``Trainer`` needs CUDA, so
these tests cover what runs on CPU: recipe/config construction, the metric-log
emitters, the hooks we wrap in upstream classes, checkpoint metadata inspection,
and the hf.py parser contract. End-to-end training runs on GPUs. The Helm chart
and launcher are covered torch-free in ``test_helm_chart.py``.
"""

from __future__ import annotations

import inspect
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("torchtitan")
pytest.importorskip("grain")

import torch  # noqa: E402

WORKLOAD_DIR = Path(__file__).resolve().parents[1]
HELM_CHART_DIR = WORKLOAD_DIR / "helm_chart"
REPO_ROOT = Path(__file__).resolve().parents[7]
CLOUDBUILD_DIR = REPO_ROOT / "cloudbuild" / "macrobenchmarks"

for _path in (HELM_CHART_DIR, CLOUDBUILD_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import torchtitan_train as tt  # noqa: E402
from metrics import calculate, raw_store  # noqa: E402
from metrics.parsers import hf as hf_parser  # noqa: E402
from torchtitan.components.checkpointer import base as ckpt_base  # noqa: E402
from torchtitan.components.checkpointer import dcp as ckpt_dcp  # noqa: E402
from torchtitan.models.common.attention.cp_attention import (  # noqa: E402
    CPInnerAttention,
)
from torchtitan.observability.metrics import MetricsProcessor  # noqa: E402
from torchtitan.trainer import Trainer  # noqa: E402
from torchtitan.training_engine import TrainingEngine  # noqa: E402

BASE_ENV = {
    "DATASET_PATH": "gs://bucket/dataset16/",
    "RUN_ID": "tt-run",
    "CKPT_WRITE_PATH": "gs://bucket/checkpoints",
    "CKPT_LOAD_PATH": "",
    "WORLD_SIZE": "16",
    "TORCHTITAN_FLAVOR": "70B",
    "SEQ_LEN": "2048",
    "MAX_STEPS": "50",
    "CHECKPOINT_WRITE_INTERVAL": "15",
    "CKPT_TO_KEEP": "2",
    "PER_DEVICE_TRAIN_BATCH_SIZE": "1",
    "GRADIENT_ACCUMULATION_STEPS": "1",
    "TRAINING_STRATEGY": "model_parallel_sharded",
    "TENSOR_PARALLEL_SIZE": "4",
    "DATA_PARALLEL_SIZE": "4",
    "DP_REPLICATE_SIZE": "1",
    "CONTEXT_PARALLEL_SIZE": "1",
    "PIPELINE_PARALLEL_SIZE": "1",
    "LOCAL_TOKENIZER_PATH": "/workload/cache/torchtitan_tokenizer/llama",
}


@pytest.fixture
def recipe_env(monkeypatch):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("RANK", raising=False)
    return monkeypatch


class TestResolveParallelism:
    def test_model_parallel_tp4_dp4(self):
        p = tt.resolve_parallelism(
            world_size=16,
            training_strategy="model_parallel_sharded",
            tp_size=4,
            dp_size=4,
        )
        assert (p.tensor_parallel_degree, p.data_parallel_shard_degree) == (4, 4)
        assert p.data_parallel_replicate_degree == 1
        assert p.pipeline_parallel_degree == p.context_parallel_degree == 1

    def test_fsdp_sharded_ignores_model_parallel_degrees(self):
        p = tt.resolve_parallelism(
            world_size=16,
            training_strategy="fsdp_sharded",
            tp_size=4,
            cp_size=2,
            pp_size=2,
        )
        assert p.data_parallel_shard_degree == 16
        assert p.tensor_parallel_degree == p.context_parallel_degree == 1
        assert p.pipeline_parallel_degree == 1

    def test_5d_layout(self):
        p = tt.resolve_parallelism(
            world_size=32,
            training_strategy="model_parallel_sharded",
            tp_size=2,
            dp_replicate_size=2,
            cp_size=2,
            pp_size=2,
        )
        assert p.data_parallel_shard_degree == 2
        assert p.num_pp_microbatches == 2

    @pytest.mark.parametrize("strategy", ["ddp", "fsdp_full", "model_parallel_full"])
    def test_rejects_unsharded_strategies(self, strategy):
        with pytest.raises(ValueError, match="TRAINING_STRATEGY must be one of"):
            tt.resolve_parallelism(world_size=8, training_strategy=strategy)

    def test_rejects_indivisible_world_size(self):
        with pytest.raises(ValueError, match="must be divisible by"):
            tt.resolve_parallelism(
                world_size=6, training_strategy="model_parallel_sharded", tp_size=4
            )

    def test_rejects_inconsistent_dp(self):
        with pytest.raises(ValueError, match="must equal WORLD_SIZE"):
            tt.resolve_parallelism(
                world_size=16,
                training_strategy="model_parallel_sharded",
                tp_size=4,
                dp_size=2,
            )

    def test_rejects_non_positive_degree(self):
        with pytest.raises(ValueError, match="CONTEXT_PARALLEL_SIZE must be >= 1"):
            tt.resolve_parallelism(
                world_size=8, training_strategy="model_parallel_sharded", cp_size=0
            )


class TestRecipe:
    def test_builds_instrumented_trainer_config(self, recipe_env):
        cfg = tt.llama3_70b_gcsfs()
        assert isinstance(cfg, tt.GcsfsBenchmarkTrainer.Config)
        assert cfg._owner is tt.GcsfsBenchmarkTrainer
        assert tt.GcsfsBenchmarkTrainer.engine_cls is tt.GcsfsTrainingEngine
        assert cfg.parallelism.tensor_parallel_degree == 4
        assert cfg.parallelism.data_parallel_shard_degree == 4
        assert cfg.training.steps == 50
        assert cfg.training.max_context_length == 2048
        assert cfg.training.num_tokens_per_microbatch_per_dp_rank == 2048
        assert cfg.training.num_tokens_per_train_step == 2048 * 4
        assert cfg.metrics.log_freq == 1
        assert cfg.validator is None
        assert cfg.profiler.enable_profiling is False
        assert cfg.hf_assets_path == BASE_ENV["LOCAL_TOKENIZER_PATH"]

    def test_dataloader_reads_parquet_over_gcs_with_packing(self, recipe_env):
        dl = tt.llama3_70b_gcsfs().dataloader
        assert isinstance(dl, tt.GcsfsGrainDataLoader.Config)
        assert dl._owner is tt.GcsfsGrainDataLoader
        assert dl.dataset_path == "gs://bucket/dataset16"
        assert isinstance(dl.dataset, tt.ConcatThenSplitPackingConfig)
        source = dl.dataset.dataset.source
        assert isinstance(source, tt.HuggingFaceStreamingSource.Config)
        assert source.path == "parquet"
        assert source.load_dataset_kwargs == {
            "data_files": "gs://bucket/dataset16/*.parquet",
            "columns": ["text"],
        }

    def test_checkpointer_writes_full_state_to_gcs(self, recipe_env):
        ckpt = tt.llama3_70b_gcsfs().checkpointer
        assert ckpt.folder == "gs://bucket/checkpoints/tt-run"
        assert ckpt.interval == 15
        assert ckpt.keep_latest_k == 2
        assert ckpt.last_save_model_only is False
        assert ckpt.initial_load_path is None
        assert ckpt.load_only is False

    @pytest.mark.parametrize(
        ("keep", "message"),
        [("1", "at least 2 checkpoint replicas"), ("-1", "cannot be negative")],
    )
    def test_upstream_rejects_invalid_ckpt_to_keep(self, recipe_env, keep, message):
        recipe_env.setenv("CKPT_TO_KEEP", keep)
        with pytest.raises(ValueError, match=message):
            tt.llama3_70b_gcsfs()

    def test_ckpt_to_keep_zero_keeps_all(self, recipe_env):
        recipe_env.setenv("CKPT_TO_KEEP", "0")
        assert tt.llama3_70b_gcsfs().checkpointer.keep_latest_k == 0

    def test_resume_loads_full_state(self, recipe_env):
        recipe_env.setenv("CKPT_LOAD_PATH", "gs://bucket/checkpoints/seed/step-1")
        ckpt = tt.llama3_70b_gcsfs().checkpointer
        assert ckpt.initial_load_path == "gs://bucket/checkpoints/seed/step-1"
        assert ckpt.initial_load_model_only is False

    def test_grad_accum_and_batch_scale_train_step_tokens(self, recipe_env):
        recipe_env.setenv("PER_DEVICE_TRAIN_BATCH_SIZE", "2")
        recipe_env.setenv("GRADIENT_ACCUMULATION_STEPS", "3")
        cfg = tt.llama3_70b_gcsfs()
        assert cfg.training.num_tokens_per_microbatch_per_dp_rank == 4096
        assert cfg.training.num_tokens_per_train_step == 3 * 4096 * 4

    def test_interval_clamped_to_steps(self, recipe_env):
        recipe_env.setenv("MAX_STEPS", "2")
        assert tt.llama3_70b_gcsfs().checkpointer.interval == 2

    def test_no_checkpoint_paths_disables_checkpointer(self, recipe_env):
        recipe_env.setenv("CKPT_WRITE_PATH", "")
        assert tt.llama3_70b_gcsfs().checkpointer is None

    @pytest.mark.parametrize("flavor", ["8B", "405B"])
    def test_rejects_unsupported_flavor(self, recipe_env, flavor):
        recipe_env.setenv("TORCHTITAN_FLAVOR", flavor)
        with pytest.raises(ValueError, match="Unsupported TORCHTITAN_FLAVOR"):
            tt.llama3_70b_gcsfs()

    def test_requires_dataset_and_run_id(self, recipe_env):
        recipe_env.setenv("DATASET_PATH", "")
        with pytest.raises(SystemExit, match="DATASET_PATH"):
            tt.llama3_70b_gcsfs()


class TestContextParallelRecipe:
    """CP > 1 must survive upstream ``Trainer.Config`` validation."""

    @pytest.mark.parametrize(
        ("attn_backend", "cp_cls"),
        [
            ("flex", tt.AllGatherKVFlexInnerAttention),
            ("varlen", tt.UlyssesVarlenInnerAttention),
        ],
    )
    def test_cp2_builds_with_cp_inner_attention(self, recipe_env, attn_backend, cp_cls):
        recipe_env.setenv("ATTN_BACKEND", attn_backend)
        recipe_env.setenv("CONTEXT_PARALLEL_SIZE", "2")
        recipe_env.setenv("DATA_PARALLEL_SIZE", "2")
        cfg = tt.llama3_70b_gcsfs()
        assert cfg.parallelism.context_parallel_degree == 2
        assert cfg.parallelism.tensor_parallel_degree == 4
        assert cfg.parallelism.data_parallel_shard_degree == 2
        inner = [layer.attention.inner_attention for layer in cfg.model.layers]
        assert inner and all(isinstance(a, CPInnerAttention.Config) for a in inner)
        assert all(a._owner is cp_cls for a in inner)

    def test_5d_recipe_builds(self, recipe_env):
        for key, value in {
            "WORLD_SIZE": "32",
            "TENSOR_PARALLEL_SIZE": "2",
            "DP_REPLICATE_SIZE": "2",
            "CONTEXT_PARALLEL_SIZE": "2",
            "PIPELINE_PARALLEL_SIZE": "2",
            "DATA_PARALLEL_SIZE": "2",
        }.items():
            recipe_env.setenv(key, value)
        cfg = tt.llama3_70b_gcsfs()
        p = cfg.parallelism
        assert (
            p.pipeline_parallel_degree,
            p.data_parallel_replicate_degree,
            p.data_parallel_shard_degree,
            p.context_parallel_degree,
            p.tensor_parallel_degree,
        ) == (2, 2, 2, 2, 2)

    def test_cp1_keeps_plain_attention(self, recipe_env):
        cfg = tt.llama3_70b_gcsfs()
        inner = cfg.model.layers[0].attention.inner_attention
        assert not isinstance(inner, CPInnerAttention.Config)


class TestUpstreamHookShape:
    """Fail loudly when TorchTitan main renames the internals this workload wraps."""

    def test_trainer_hooks(self):
        for name in ("microbatch_generator", "train_step", "train", "engine_cls"):
            assert hasattr(Trainer, name), name
        assert list(inspect.signature(Trainer.train_step).parameters) == [
            "self",
            "data_iterator",
        ]

    def test_engine_checkpoint_hooks(self):
        assert (
            "last_step" in inspect.signature(TrainingEngine.save_checkpoint).parameters
        )
        assert hasattr(TrainingEngine, "load_checkpoint")
        for name in ("_find_load_step", "_create_checkpoint_id"):
            assert hasattr(ckpt_dcp.CheckpointManager, name), name

    def test_metrics_log_signature(self):
        params = list(inspect.signature(MetricsProcessor.log).parameters)
        assert params[:5] == [
            "self",
            "step",
            "global_avg_loss",
            "global_max_loss",
            "grad_norm",
        ]

    def test_purge_thread_log_message(self):
        assert tt._DELETED_MSG in inspect.getsource(ckpt_base.purge_thread)
        assert ckpt_base.logger.name == tt._CHECKPOINTER_LOGGER


# ---------------------------------------------------------------------------
# Engine / trainer shells: real upstream CheckpointManager logic on local disk,
# with the CUDA-only constructors and the upstream save/load bodies stubbed.
# ---------------------------------------------------------------------------


def _engine(folder, *, initial_load_path=None):
    ckpt = object.__new__(ckpt_dcp.CheckpointManager)
    ckpt.folder = str(folder)
    ckpt._storage = ckpt_dcp._FilesystemCheckpointStorage()
    ckpt.exclude_from_loading = []
    engine = object.__new__(tt.GcsfsTrainingEngine)
    engine.checkpointer = ckpt
    engine.config = SimpleNamespace(
        checkpointer=SimpleNamespace(initial_load_path=initial_load_path)
    )
    return engine


def _trainer():
    trainer = object.__new__(tt.GcsfsBenchmarkTrainer)
    trainer._last_loss = 11.8234
    trainer._fetch_count = 0
    trainer._fetch_wait_s = 0.0
    trainer._data_wait_total_s = 0.0
    trainer._step_wall_s = 0.0
    trainer._samples_per_step = 4
    trainer.engine = SimpleNamespace(num_completed_steps=1)
    return trainer


def _make_step(folder: Path, step: int, *, resumable: bool = True) -> None:
    step_dir = folder / f"step-{step}"
    step_dir.mkdir(parents=True)
    if resumable:
        (step_dir / ".metadata").write_bytes(b"")


@pytest.fixture
def stub_upstream(monkeypatch):
    """Upstream save/load/step bodies need CUDA + a real model; make them no-ops."""
    monkeypatch.setattr(TrainingEngine, "load_checkpoint", lambda self: True)
    monkeypatch.setattr(
        TrainingEngine, "save_checkpoint", lambda self, *, last_step=False: True
    )
    monkeypatch.setattr(Trainer, "train_step", lambda self, it: None)
    monkeypatch.setattr(tt.GrainDataLoader, "__init__", lambda self, cfg, **kw: None)
    return monkeypatch


class TestCheckpointHooks:
    @pytest.mark.parametrize(
        ("existing", "expected_step"),
        [
            ({20: True, 40: True, 50: False}, 40),  # newest resumable; 50 half-written
            ({20: False}, -1),  # nothing resumable -> initial_load_path
            ({}, -1),  # no folder -> initial_load_path
        ],
    )
    def test_load_target_matches_upstream(self, tmp_path, existing, expected_step):
        folder = tmp_path / "run"
        for step, resumable in existing.items():
            _make_step(folder, step, resumable=resumable)
        seed = str(tmp_path / "seed" / "step-1")
        expected = (
            (str(folder / f"step-{expected_step}"), expected_step)
            if expected_step != -1
            else (seed, -1)
        )
        engine = _engine(folder, initial_load_path=seed)
        assert engine._resolve_load_target() == expected

    @pytest.mark.parametrize(("step", "checks_dataloader"), [(20, True), (0, False)])
    def test_load_reports_step_dir(
        self, tmp_path, stub_upstream, capsys, step, checks_dataloader
    ):
        folder = tmp_path / "run"
        _make_step(folder, step)
        engine = _engine(folder)
        checked = []
        stub_upstream.setattr(
            engine, "_skip_incompatible_dataloader_state", checked.append
        )
        assert engine.load_checkpoint() is True
        step_dir = str(folder / f"step-{step}")
        assert checked == ([step_dir] if checks_dataloader else [])
        assert capsys.readouterr().out.count(f"Path: {step_dir}") == 2

    def test_save_reports_size(self, tmp_path, stub_upstream, capsys):
        engine = _engine(tmp_path)
        engine.num_completed_steps = 20
        (tmp_path / "step-20").mkdir()
        (tmp_path / "step-20" / "shard").write_bytes(b"x" * 10)
        assert engine.save_checkpoint() is True
        out = capsys.readouterr().out
        assert f"Finished saving checkpoint to {tmp_path}/step-20" in out
        assert "Checkpoint Size : Rank : 0 : Step : 20 : Bytes : 10 :" in out

    def test_size_failure_does_not_fail_run(self, tmp_path, stub_upstream, capsys):
        engine = _engine(tmp_path)
        engine.num_completed_steps = 20

        def _boom(*args, **kwargs):
            raise OSError("transient")

        stub_upstream.setattr(tt.fsspec.core, "url_to_fs", _boom)
        assert engine.save_checkpoint() is True
        out = capsys.readouterr().out
        assert "Finished saving checkpoint to" in out
        assert "Checkpoint Size" not in out


def _log_purge(path: str, seconds: float) -> None:
    tt._install_delete_bridge()
    logger = logging.getLogger(tt._CHECKPOINTER_LOGGER)
    old_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        logger.info(tt._DELETED_MSG, path, seconds)
    finally:
        logger.setLevel(old_level)


class TestMetricEmitters:
    def test_delete_bridge_reemits_purge_timing(self, capsys):
        tt._install_delete_bridge()  # idempotent with the call in _log_purge
        _log_purge("gs://b/ckpts/run/step-25", 3.5)
        out = capsys.readouterr().out
        assert out.count("Finished deleting checkpoint") == 1
        assert (
            "Finished deleting checkpoint gs://b/ckpts/run/step-25 in 3.5000 seconds "
            "for global_step 25 from rank 0"
        ) in out

    def test_timed_batches_emit_data_wait_lines(self, capsys):
        trainer = _trainer()
        assert list(trainer._timed_batches(["a", "b"])) == ["a", "b"]
        lines = capsys.readouterr().out.splitlines()
        assert lines[0].startswith(
            "Data Wait : Rank : 0 : Fetch : 0 : Action : setup_train_dataloader"
        )
        assert (
            "Fetch : 2 : Action : [_TrainingEpochLoop].train_dataloader_next"
            in lines[2]
        )
        assert trainer._fetch_count == 2

    def test_saved_dataloader_dp_ranks(self, tmp_path):
        import torch.distributed.checkpoint as dcp

        state = {
            "dataloader": {
                "version": 1,
                "dp_world_size": 2,
                "dp_rank_0": {"epoch": 0, "pos": torch.tensor([1])},
                "dp_rank_1": {"epoch": 0, "pos": torch.tensor([2])},
            },
            "model": {"w": torch.zeros(2)},
        }
        dcp.save(state, checkpoint_id=str(tmp_path / "step-1"))
        assert tt.saved_dataloader_dp_ranks(str(tmp_path / "step-1")) == {0, 1}


class TestMetricsParserCompatibility:
    def test_emitted_lines_parse_into_summary_row(
        self, tmp_path, stub_upstream, capsys
    ):
        # Every line comes from the workload's real hooks, so format drift in
        # torchtitan_train.py breaks this test.
        tt.GcsfsGrainDataLoader(SimpleNamespace(dataset_path="gs://b/dataset16"))
        trainer = _trainer()
        list(trainer._timed_batches(range(2)))
        trainer.train_step(iter(()))
        trainer._print_summary()

        folder = tmp_path / "ckpts" / "run"
        (folder / "step-2").mkdir(parents=True)
        (folder / "step-2" / "shard").write_bytes(b"x" * 10)
        saver = _engine(folder)
        saver.num_completed_steps = 2
        saver.save_checkpoint()
        _log_purge(str(folder / "step-1"), 1.2)

        seed = str(tmp_path / "seed" / "step-1")
        loader = _engine(tmp_path / "fresh", initial_load_path=seed)
        stub_upstream.setattr(loader, "_skip_incompatible_dataloader_state", print)
        loader.load_checkpoint()
        lines = capsys.readouterr().out.splitlines()

        entries = [
            hf_parser.LogEntry(message=line, timestamp=1700000000.0 + i)
            for i, line in enumerate(lines)
        ]
        parsed = hf_parser.parse_entries(
            entries, run_id="tt-test-run", checkpoint_location=str(tmp_path)
        )
        raw_store.write_raw_metrics(parsed, str(tmp_path / "raw"))
        tables = raw_store.read_raw_metrics(str(tmp_path / "raw"))
        row = calculate.build_summary_row(
            run_id="tt-test-run",
            workload_name="torchtitan",
            requirements="gcsfs",
            step_rows=tables.step_rows,
            write_rows=tables.write_rows,
            restore_rows=tables.restore_rows,
            delete_rows=tables.delete_rows,
            dl_rows=tables.dl_rows,
            size_rows=tables.size_rows,
            system_rows=tables.system_rows,
            data_wait_rows=tables.data_wait_rows,
            dataset_build_rows=tables.dataset_build_rows,
            ray_data_iteration_rows=tables.ray_data_iteration_rows,
            dimensions={"nodes": 2, "ranks_per_node": 8},
        )
        for column in (
            "dataset_build_time",
            "checkpoint_write_time_avg",
            "checkpoint_restore_time_avg",
        ):
            assert row[column] not in (None, ""), column
        assert float(row["checkpoint_delete_time_avg"]) == pytest.approx(1.2)
        assert int(row["checkpoint_size_bytes"]) == 10
        assert len(tables.step_rows) == 1
        assert len(tables.data_wait_rows) == 3  # setup + 2 fetches
