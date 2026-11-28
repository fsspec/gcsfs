# Known Accelerator and Framework Limitations

Each entry in `_common/limitations.py` cites an anchor in this file with a
minimal standalone repro and error output.

<a id="lightning-tpu"></a>
## lightning-tpu

- **Group:** `checkpointing/pytorch_lightning`
- **Accelerator:** `tpu` (TorchTPU)
- **Status:** Permanent skip (Lightning does not support TorchTPU).

PyTorch Lightning does not support the TorchTPU (`torch_tpu`) backend:

1. `SingleDeviceStrategy` and `DDPStrategy` hard-code CUDA/CPU device handling
   and do not recognize `torch.device("tpu")`.
2. `FSDPStrategy` explicitly rejects non-CUDA accelerators (`MisconfigurationException`
   requiring `CUDAAccelerator`).
3. Lightning's launcher/signal setup resets `libtpu`'s `SIGPROF` handler,
   causing runtime failures under TorchTPU.

Because Lightning only targets `torch_xla` for TPUs (which is outside the
TorchTPU stack used by these benchmarks), all `checkpointing/pytorch_lightning`
cases skip on TPU hosts and run on CPU and CUDA GPU hosts.
