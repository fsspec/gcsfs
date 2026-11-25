"""Tiny HF models and benchmark parameters for the checkpointing/pytorch tests."""

_SIZE_ALIASES = {
    "dpr": "data_parallel_replicate_size",
    "dp": "data_parallel_size",
    "tp": "tensor_parallel_size",
    "pp": "pipeline_parallel_size",
    "ep": "expert_parallel_size",
}


def make_params(model_id: str = "m", **fields):
    """PyTorchCheckpointParameters with test defaults; dpr/dp/tp/pp/ep alias sizes."""
    from gcsfs.tests.perf.subsystembenchmarks.checkpointing.pytorch.parameters import (
        PyTorchCheckpointParameters,
    )

    defaults = dict(
        name="c",
        bucket_name="",
        bucket_type="regional",
        rounds=1,
        scenario="checkpoint_read",
        framework="pytorch",
        strategy="fsdp",
    )
    fields = {_SIZE_ALIASES.get(k, k): v for k, v in fields.items()}
    return PyTorchCheckpointParameters(model_id=model_id, **{**defaults, **fields})


def tiny_model_dir(
    tmp_path, kind: str = "llama", num_layers: int = 8, num_experts: int = 16
) -> str:
    import torch
    from transformers import AutoModelForCausalLM, LlamaConfig, OlmoeConfig

    if kind == "llama":
        cfg = LlamaConfig(
            hidden_size=64,
            num_hidden_layers=num_layers,
            num_attention_heads=8,
            num_key_value_heads=8,
            intermediate_size=128,
            vocab_size=256,
            max_position_embeddings=512,
            rope_scaling=None,
        )
    elif kind == "olmoe":
        cfg = OlmoeConfig(
            hidden_size=64,
            num_hidden_layers=num_layers,
            num_attention_heads=8,
            num_key_value_heads=8,
            intermediate_size=32,
            num_experts=num_experts,
            num_experts_per_tok=2,
            vocab_size=256,
            max_position_embeddings=512,
            eos_token_id=1,
        )
    else:
        raise ValueError(f"Unsupported model kind {kind!r}")

    model = AutoModelForCausalLM.from_config(cfg, dtype=torch.float32)
    model.save_pretrained(tmp_path)
    return str(tmp_path)
