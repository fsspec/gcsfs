"""Tests for the TorchTitan ``main`` gcsfs macrobenchmark recipe.

TorchTitan ``main`` needs a PyTorch nightly and its ``Trainer`` needs CUDA, so
these tests cover what runs on CPU: recipe/config construction, the metric-log
emitters, the hooks we wrap in upstream classes, checkpoint metadata inspection,
the hf.py parser contract, and the Helm chart. End-to-end training runs on GPUs.
"""

from __future__ import annotations

import inspect
import logging
import shutil
import subprocess
import sys
from pathlib import Path

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
    "CHECKPOINT_WRITE_INTERVAL": "25",
    "CKPT_TO_KEEP": "1",
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
        assert ckpt.interval == 25
        assert ckpt.keep_latest_k == 2  # upstream minimum when retention is on
        assert ckpt.last_save_model_only is False
        assert ckpt.initial_load_path is None
        assert ckpt.load_only is False

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


class TestMetricEmitters:
    def test_delete_bridge_reemits_purge_timing(self, capsys):
        tt._install_delete_bridge()
        tt._install_delete_bridge()  # idempotent
        logger = logging.getLogger(tt._CHECKPOINTER_LOGGER)
        old_level = logger.level
        logger.setLevel(logging.INFO)
        try:
            logger.info(tt._DELETED_MSG, "gs://b/ckpts/run/step-25", 3.5)
        finally:
            logger.setLevel(old_level)
        out = capsys.readouterr().out
        assert out.count("Finished deleting checkpoint") == 1
        assert (
            "Finished deleting checkpoint gs://b/ckpts/run/step-25 in 3.5000 seconds "
            "for global_step 25 from rank 0"
        ) in out

    def test_timed_batches_emit_data_wait_lines(self, capsys):
        trainer = object.__new__(tt.GcsfsBenchmarkTrainer)
        trainer._fetch_count = 0
        trainer._fetch_wait_s = 0.0
        trainer._data_wait_total_s = 0.0
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
    def test_emitted_lines_parse_into_summary_row(self, tmp_path, capsys):
        trainer = object.__new__(tt.GcsfsBenchmarkTrainer)
        trainer._fetch_count = 0
        trainer._fetch_wait_s = 0.0
        trainer._data_wait_total_s = 0.0
        trainer._step_wall_s = 2.0
        list(trainer._timed_batches(range(2)))
        trainer._print_summary()
        emitted = capsys.readouterr().out.splitlines()

        lines = [
            "Dataset Build : Rank : 0 : Duration : 0.8123 seconds : Path: gs://b/dataset16",
            *emitted,
            "Global Rank: 0 | Step: 1 | Loss: 11.8234 | Step Time: 2.1500s | Throughput: 7.44 samples/s",
            "Checkpoint Save : Rank: 0 : Step: 2 : Start time: 1700000000.0000 seconds: Path: gs://b/ckpts/run/step-2",
            "Finished saving checkpoint to gs://b/ckpts/run/step-2 in 14.5000 seconds for global_step 2 from rank 0",
            "Checkpoint Size : Rank : 0 : Step : 2 : Bytes : 847000000000 : Path: gs://b/ckpts/run/step-2",
            "Finished deleting checkpoint gs://b/ckpts/run/step-1 in 1.2000 seconds for global_step 1 from rank 0",
            (
                "Checkpoint Restore Start : Rank : 0 : Start time: 1700000020.0000 seconds : "
                "Path: gs://b/ckpts/seed/step-1"
            ),
            (
                "Finished restoring checkpoint : Rank : 0 : Duration: 9.8000 seconds : "
                "End Time: 1700000029.8000 seconds : Path: gs://b/ckpts/seed/step-1"
            ),
        ]
        entries = [
            hf_parser.LogEntry(message=line, timestamp=1700000000.0 + i)
            for i, line in enumerate(lines)
        ]
        parsed = hf_parser.parse_entries(
            entries, run_id="tt-test-run", checkpoint_location="gs://b/ckpts"
        )
        raw_store.write_raw_metrics(parsed, str(tmp_path))
        tables = raw_store.read_raw_metrics(str(tmp_path))
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
        assert float(row["dataset_build_time"]) == pytest.approx(0.8123)
        assert float(row["checkpoint_write_time_avg"]) == pytest.approx(14.5)
        assert float(row["checkpoint_restore_time_avg"]) == pytest.approx(9.8)
        assert float(row["checkpoint_delete_time_avg"]) == pytest.approx(1.2)
        assert int(row["checkpoint_size_bytes"]) == 847000000000
        assert len(tables.data_wait_rows) == 3  # setup + 2 fetches


class TestHelmChartRendering:
    def test_helm_template_renders(self):
        helm_bin = shutil.which("helm")
        if not helm_bin:
            pytest.skip("helm binary not available")
        rendered = subprocess.run(
            [
                helm_bin,
                "template",
                "tt-llama70b",
                str(HELM_CHART_DIR),
                "-f",
                str(HELM_CHART_DIR / "values_base.yaml"),
                "--set",
                "workload.nodes=2",
                "--set",
                "workload.ranksPerNode=8",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        assert "kind: JobSet" in rendered
        assert "parallelism: 2" in rendered
        assert "nvidia.com/gpu: 8" in rendered
        assert "nccl-plugin-installer" in rendered
        assert "torchtitan_train.py: |" in rendered
        assert (
            "torchtitan @ git+https://github.com/pytorch/torchtitan.git@main"
            in rendered
        )
        assert "-m torchtitan.train" in rendered
        assert 'name: ATTN_BACKEND\n                value: "flex"' in rendered
        assert (
            'name: TORCH_NIGHTLY_INDEX\n                value: "https://download.pytorch.org/whl/nightly/cu132"'
            in rendered
        )
        for removed in (
            "TORCH_DISTRIBUTED_TARGET",
            "DATALOADER_NUM_WORKERS",
            "SHUFFLE_MAX_BUFFER_INPUT_SHARDS",
        ):
            assert removed not in rendered
