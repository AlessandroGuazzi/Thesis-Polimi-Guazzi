import os
import torch
from train_resnet import WSTSResNet18

def main():
    checkpoint_path = 'checkpoints/wsts_resnet18_latest.pth'
    if not os.path.exists(checkpoint_path):
        raise FileNotFoundError(f"Checkpoint non trovato: {checkpoint_path}. Esegui prima train_resnet.py")
        
    print("Caricamento checkpoint PyTorch...")
    checkpoint = torch.load(checkpoint_path, map_location='cpu')
    
    # Inizializza il modello puro
    model = WSTSResNet18()
    model.load_state_dict(checkpoint['model_state_dict'])
    
    # 1. CONGELAMENTO BATCH NORMALIZATION
    model.eval()
    
    # 2. GRAFO STATICO: Input fissato a 120 canali (Data-Level Fusion)
    # L'input previsto è (Batch=1, Canali=120, Altezza=64, Larghezza=64)
    dummy_input = torch.randn(1, 120, 64, 64, dtype=torch.float32)
    
    output_path = 'checkpoints/wsts_model.onnx'
    print(f"Esportazione modello ONNX in {output_path}...")
    
    # 3. ONNX EXPORT
    torch.onnx.export(
        model,
        dummy_input,
        output_path,
        export_params=True,
        opset_version=14,          
        do_constant_folding=True,  
        input_names=['input_telemetry'], # Rinominiamo l'input per chiarezza nel worker spaziale
        output_names=['fire_prediction_mask'],
        dynamic_axes=None          # Nessun asse dinamico = Nessun OOMKill su Kubernetes!
    )
    
    print("✅ Esportazione ONNX completata! Il file wsts_model.onnx è pronto per il volo spaziale.")

if __name__ == '__main__':
    main()