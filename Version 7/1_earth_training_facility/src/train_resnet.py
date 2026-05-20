import os
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision.models as models
from torch.utils.data import DataLoader

# Importiamo il dataset ufficiale dei ricercatori
from dataloader.FireSpreadDataset import FireSpreadDataset

class WSTSResNet18(nn.Module):
    """
    Fully Convolutional ResNet-18 variant for 64x64 spatial masks.
    Architettura ottimizzata per tensori Early Fusion a 120 canali.
    """
    def __init__(self):
        super().__init__()
        
        resnet = models.resnet18(weights=None)
        
        # Modifichiamo il primo layer per accettare 120 canali (Output esatto di remove_duplicate_features)
        self.conv1 = nn.Conv2d(120, 64, kernel_size=7, stride=2, padding=3, bias=False)
        self.bn1 = resnet.bn1
        self.relu = resnet.relu
        self.maxpool = resnet.maxpool
        
        self.layer1 = resnet.layer1
        self.layer2 = resnet.layer2
        self.layer3 = resnet.layer3
        self.layer4 = resnet.layer4
        
        # Decoder per riportare la risoluzione a 64x64
        self.decoder = nn.Sequential(
            nn.Upsample(scale_factor=2, mode='bilinear', align_corners=False),
            nn.Conv2d(512, 256, kernel_size=3, padding=1),
            nn.BatchNorm2d(256),
            nn.ReLU(inplace=True),
            
            nn.Upsample(scale_factor=2, mode='bilinear', align_corners=False),
            nn.Conv2d(256, 128, kernel_size=3, padding=1),
            nn.BatchNorm2d(128),
            nn.ReLU(inplace=True),
            
            nn.Upsample(scale_factor=2, mode='bilinear', align_corners=False),
            nn.Conv2d(128, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(inplace=True),
            
            nn.Upsample(scale_factor=2, mode='bilinear', align_corners=False),
            nn.Conv2d(64, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(inplace=True),
            
            nn.Upsample(scale_factor=2, mode='bilinear', align_corners=False),
            nn.Conv2d(32, 1, kernel_size=3, padding=1)
        )

    def forward(self, x):
        # La normalizzazione non è più qui! Avviene nativamente nel Dataloader
        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.maxpool(x)
        
        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)
        x = self.layer4(x)
        
        x = self.decoder(x)
        return x

def main():
    # Su Colab, scompatteremo lo zip qui dentro
    data_path = os.getenv('WSTS_DATA_PATH', '/content/data_training/WSTS_Output')
    
    print(f"Inizializzazione del FireSpreadDataset da: {data_path}")
    
    # Inizializziamo il dataset ufficiale per il TRAINING (2018 e 2019)
    try:
        dataset = FireSpreadDataset(
            data_dir=data_path,
            included_fire_years=[2018, 2019],  # Dati di addestramento rigidi
            n_leading_observations=5,          # T = 5 giorni
            crop_side_length=64,               # Fissiamo la risoluzione spaziale
            load_from_hdf5=True,               # Leggiamo dai nostri nuovi file HDF5
            is_train=True,                     # Abilita l'augmentation geometrica
            remove_duplicate_features=True,    # Fondamentale: compatta a 120 canali e rimuove i duplicati statici
            stats_years=[2018, 2019]           # Anni usati per calcolare mean/std in utils.py
        )
    except Exception as e:
        print(f"Errore caricamento dataset: Assicurati che i file HDF5 siano stati scompattati in {data_path}/2018/ ecc.")
        raise e

    dataloader = DataLoader(dataset, batch_size=32, shuffle=True, num_workers=4, pin_memory=True)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = WSTSResNet18().to(device)
    
    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=1e-3)
    
    epochs = 5
    print(f"Inizio addestramento su {device} per {epochs} epoche...")
    
    model.train()
    for epoch in range(epochs):
        epoch_loss = 0.0
        for batch_idx, (features, labels) in enumerate(dataloader):
            # Il dataset restituisce tuple, portiamo tutto su GPU
            features, labels = features.to(device), labels.to(device)
            
            # Assicuriamoci che le label abbiano la dimensione corretta per la loss [Batch, 1, 64, 64]
            labels = labels.unsqueeze(1).float()
            
            optimizer.zero_grad()
            outputs = model(features)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            epoch_loss += loss.item()
            
        print(f"Epoca {epoch+1}/{epochs}, Loss Media: {epoch_loss/max(1, len(dataloader)):.4f}")
        
    os.makedirs('checkpoints', exist_ok=True)
    
    # Salviamo solo i pesi (niente più mean/std nel checkpoint)
    torch.save({
        'model_state_dict': model.state_dict(),
    }, 'checkpoints/wsts_resnet18_latest.pth')
    print("Addestramento completato. Modello salvato.")

if __name__ == '__main__':
    main()