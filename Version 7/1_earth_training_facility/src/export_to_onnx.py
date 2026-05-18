import os
import torch
from train_resnet import WSTSResNet18

def main():
    checkpoint_path = 'checkpoints/wsts_resnet18_latest.pth'
    if not os.path.exists(checkpoint_path):
        raise FileNotFoundError(f"Checkpoint not found at {checkpoint_path}. Run train_resnet.py first.")
        
    print("Loading checkpoint...")
    checkpoint = torch.load(checkpoint_path, map_location='cpu')
    mean = checkpoint['mean']
    std = checkpoint['std']
    
    model = WSTSResNet18(mean, std)
    model.load_state_dict(checkpoint['model_state_dict'])
    
    # 1. BATCH NORMALIZATION FREEZING
    # Setting model to eval mode mathematically freezes BatchNorm and Dropout layers.
    model.eval()
    
    # 2. STATIC GRAPH / DYNAMIC AXES DISABLED
    # Fix input dimensions to strictly 64x64 for maximum edge efficiency
    # The expected shape is (Batch, Channels, Height, Width) -> (1, 115, 64, 64)
    dummy_input = torch.randn(1, 115, 64, 64, dtype=torch.float32)
    
    output_path = 'checkpoints/wsts_model.onnx'
    print(f"Exporting ONNX model to {output_path}...")
    
    # 3. ONNX OPSET 14
    torch.onnx.export(
        model,
        dummy_input,
        output_path,
        export_params=True,
        opset_version=14,          # Stable ONNX opset
        do_constant_folding=True,  # Fold constant values for optimization
        input_names=['input'],
        output_names=['output'],
        dynamic_axes=None          # Strictly disable dynamic axes to force a fixed graph
    )
    
    print("Export complete. The edge worker can now load this static graph directly.")

if __name__ == '__main__':
    main()
