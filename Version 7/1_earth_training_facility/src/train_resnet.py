import os
import h5py
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision.models as models
from torch.utils.data import Dataset, DataLoader

class WSTSDataset(Dataset):
    """
    Lazy loads the WSTS HDF5 dataset to avoid OOM on 50GB files.
    """
    def __init__(self, h5_path):
        self.h5_path = h5_path
        self.file = None
        
        # Open temporarily to get the length of the dataset
        with h5py.File(self.h5_path, 'r') as f:
            if 'features' in f:
                self.length = f['features'].shape[0]
            else:
                self.length = 0

    def _open_file(self):
        # We open the file lazily in __getitem__ to ensure thread-safety across DataLoader workers
        if self.file is None:
            self.file = h5py.File(self.h5_path, 'r')

    def __len__(self):
        return self.length

    def __getitem__(self, idx):
        self._open_file()
        
        # Read only the requested index from disk
        features = self.file['features'][idx]  # Shape: (5, 23, 64, 64)
        label = self.file['labels'][idx]       # Shape: (1, 64, 64)
        
        # Flatten the temporal dimension into the channel dimension -> (115, 64, 64)
        features = features.reshape(-1, 64, 64)
        
        return torch.tensor(features, dtype=torch.float32), torch.tensor(label, dtype=torch.float32)

class NormalizationLayer(nn.Module):
    """
    Bakes the dataset's Mean and Std directly into the model graph
    so the Edge worker doesn't have to scale the data dynamically.
    """
    def __init__(self, mean, std):
        super().__init__()
        # Register as buffers so they are exported with the ONNX graph
        self.register_buffer('mean', torch.tensor(mean, dtype=torch.float32).view(1, -1, 1, 1))
        self.register_buffer('std', torch.tensor(std, dtype=torch.float32).view(1, -1, 1, 1))

    def forward(self, x):
        return (x - self.mean) / self.std

class WSTSResNet18(nn.Module):
    """
    Fully Convolutional ResNet-18 variant for 64x64 spatial masks.
    """
    def __init__(self, mean, std):
        super().__init__()
        self.norm = NormalizationLayer(mean, std)
        
        resnet = models.resnet18(weights=None)
        
        # Modify first layer for 115 channels (5 days * 23 channels)
        self.conv1 = nn.Conv2d(115, 64, kernel_size=7, stride=2, padding=3, bias=False)
        self.bn1 = resnet.bn1
        self.relu = resnet.relu
        self.maxpool = resnet.maxpool
        
        self.layer1 = resnet.layer1
        self.layer2 = resnet.layer2
        self.layer3 = resnet.layer3
        self.layer4 = resnet.layer4
        
        # Decoder to upsample back to 64x64
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
        x = self.norm(x)
        
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

def calculate_normalization_params(dataloader, num_batches=100):
    """
    Estimate mean and std from a subset of the data.
    """
    print("Calculating dataset mean and std...")
    channels_sum = 0
    channels_squared_sum = 0
    num_batches = min(num_batches, len(dataloader))
    
    if num_batches == 0:
        return np.zeros(115), np.ones(115)
        
    for i, (features, _) in enumerate(dataloader):
        if i >= num_batches:
            break
        # features shape: (Batch, 115, 64, 64)
        channels_sum += torch.mean(features, dim=[0, 2, 3])
        channels_squared_sum += torch.mean(features**2, dim=[0, 2, 3])
        
    mean = channels_sum / num_batches
    std = (channels_squared_sum / num_batches - mean**2) ** 0.5
    # Replace zeros with ones to prevent division by zero
    std[std == 0] = 1.0
    return mean.numpy(), std.numpy()

def main():
    data_path = os.getenv('WSTS_DATA_PATH', 'wsts.hdf5')
    if not os.path.exists(data_path):
        print(f"Warning: {data_path} not found. Creating a dummy file for demonstration.")
        # Create a dummy hdf5 for testing if needed
        with h5py.File(data_path, 'w') as f:
            f.create_dataset('features', shape=(10, 5, 23, 64, 64), dtype='f4')
            f.create_dataset('labels', shape=(10, 1, 64, 64), dtype='f4')
            
    dataset = WSTSDataset(data_path)
    dataloader = DataLoader(dataset, batch_size=32, shuffle=True, num_workers=4, pin_memory=True)
    
    mean, std = calculate_normalization_params(dataloader)
    print("Normalization parameters calculated.")
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = WSTSResNet18(mean, std).to(device)
    
    # Using BCEWithLogitsLoss because the model outputs raw logits (no sigmoid at the end)
    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=1e-3)
    
    epochs = 5
    print(f"Starting training on {device} for {epochs} epochs...")
    
    model.train()
    for epoch in range(epochs):
        epoch_loss = 0.0
        for batch_idx, (features, labels) in enumerate(dataloader):
            features, labels = features.to(device), labels.to(device)
            
            optimizer.zero_grad()
            outputs = model(features)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            epoch_loss += loss.item()
            
        print(f"Epoch {epoch+1}/{epochs}, Loss: {epoch_loss/max(1, len(dataloader)):.4f}")
        
    os.makedirs('checkpoints', exist_ok=True)
    torch.save({
        'model_state_dict': model.state_dict(),
        'mean': mean,
        'std': std
    }, 'checkpoints/wsts_resnet18_latest.pth')
    print("Training complete. Model saved to checkpoints/wsts_resnet18_latest.pth")

if __name__ == '__main__':
    main()
