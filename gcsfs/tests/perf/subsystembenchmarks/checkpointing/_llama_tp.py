def apply_llama_tp(model, tp_mesh) -> None:
    """Applies tensor parallelism to a LLaMA model: shards embed_tokens, lm_head, and decoder layers.

    Top-level modules that are absent (e.g. on a pipeline stage) are skipped.
    """
    from torch.distributed.tensor import Replicate
    from torch.distributed.tensor.parallel import (
        ColwiseParallel,
        RowwiseParallel,
        parallelize_module,
    )

    top_level_plan = {
        "model.embed_tokens": RowwiseParallel(
            input_layouts=Replicate(), output_layouts=Replicate()
        ),
        "lm_head": ColwiseParallel(output_layouts=Replicate()),
    }
    present = {name for name, _ in model.named_modules()}
    parallelize_module(
        model,
        tp_mesh,
        {fqn: style for fqn, style in top_level_plan.items() if fqn in present},
    )

    layer_plan = {
        "self_attn.q_proj": ColwiseParallel(),
        "self_attn.k_proj": ColwiseParallel(),
        "self_attn.v_proj": ColwiseParallel(),
        "self_attn.o_proj": RowwiseParallel(),
        "mlp.gate_proj": ColwiseParallel(),
        "mlp.up_proj": ColwiseParallel(),
        "mlp.down_proj": RowwiseParallel(),
    }
    # children() yields the layers of both an nn.ModuleList and a stage's nn.ModuleDict.
    for layer in model.model.layers.children():
        parallelize_module(layer, tp_mesh, layer_plan)
