import torch
import bitsandbytes as bnb

print(f"PyTorch CUDA: {torch.cuda.is_available()}")
print(f"GPU Compute Capability: {torch.cuda.get_device_capability()}")