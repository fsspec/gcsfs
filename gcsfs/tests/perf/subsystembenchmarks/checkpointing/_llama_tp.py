def apply_llama_tp(model, tp_mesh) -> None:
    """Applies tensor parallelism to a LLaMA model: shards embed_tokens, lm_head, and decoder layers."""
    from torch.distributed.tensor import Replicate
    from torch.distributed.tensor.parallel import (
        ColwiseParallel,
        RowwiseParallel,
        parallelize_module,
    )

    parallelize_module(
        model,
        tp_mesh,
        {
            "model.embed_tokens": RowwiseParallel(
                input_layouts=Replicate(), output_layouts=Replicate()
            ),
            "lm_head": ColwiseParallel(output_layouts=Replicate()),
        },
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
    for layer in model.model.layers:
        parallelize_module(layer, tp_mesh, layer_plan)
