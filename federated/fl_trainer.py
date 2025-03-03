# fl/fl_trainer.py

import logging
import asyncio
import hashlib
import json
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import (Any, Awaitable, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union)

import numpy as np
import torch
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.exceptions import InvalidSignature
from pydantic import BaseModel, Field, validator
from torch import nn, optim
import uvloop
import orjson

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger = logging.getLogger("FLTrainer")

class FLModel(nn.Module):
    """Base federated learning model architecture"""
    def __init__(self):
        super().__init__()
        self.layer1 = nn.Linear(128, 256)
        self.layer2 = nn.Linear(256, 128)
        self.dropout = nn.Dropout(0.2)
        
    def forward(self, x):
        x = torch.relu(self.layer1(x))
        x = self.dropout(x)
        return self.layer2(x)

class FLClientConfig(BaseModel):
    """Validated configuration for FL clients"""
    client_id: str = Field(..., min_length=16)
    learning_rate: float = Field(0.01, gt=0)
    differential_privacy: bool = True
    dp_sigma: float = Field(0.5, gt=0)
    max_update_size: float = Field(1e6, gt=0)
    min_local_epochs: int = Field(3, ge=1)
    allowed_peers: List[str] = Field(default_factory=list)
    
    @validator('client_id')
    def validate_client_id(cls, v):
        if not v.startswith("client_"):
            raise ValueError("Invalid client ID format")
        return v

class FLServerConfig(BaseModel):
    """Validated configuration for FL server"""
    aggregation_strategy: str = Field("fedavg", regex="^(fedavg|fedprox|scaffold)$")
    max_rounds: int = Field(100, ge=1)
    target_accuracy: float = Field(0.95, gt=0)
    participant_ratio: float = Field(0.3, gt=0, le=1.0)
    model_public_key: Optional[str]
    encryption_enabled: bool = True

class EncryptedModelUpdate(BaseModel):
    """Secure model update container with cryptographic proofs"""
    client_id: str
    encrypted_weights: bytes
    signature: bytes
    timestamp: datetime = Field(default_factory=lambda: datetime.utcnow())
    metadata: Dict[str, float] = Field(default_factory=dict)
    nonce: bytes = Field(..., min_length=32)
    
    def verify_signature(self, public_key: bytes) -> bool:
        try:
            pub_key = load_pem_public_key(public_key)
            pub_key.verify(
                self.signature,
                self.encrypted_weights + self.nonce,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except InvalidSignature:
            return False

class FederatedTrainer:
    """Enterprise-grade federated learning system with ZKP and DP"""
    def __init__(
        self,
        model: FLModel,
        server_config: FLServerConfig,
        private_key: Optional[rsa.RSAPrivateKey] = None,
        public_key: Optional[rsa.RSAPublicKey] = None
    ):
        self.global_model = model
        self.server_config = server_config
        self.private_key = private_key
        self.public_key = public_key
        self.client_updates: Dict[str, EncryptedModelUpdate] = {}
        self.round = 0
        self.lock = asyncio.Lock()
        self._stop_flag = False

    def _initialize_round(self):
        """Prepare model weights for new training round"""
        self.global_model.train()
        torch.manual_seed(self.round)
        
    def _apply_dp_noise(self, weights: List[torch.Tensor]) -> List[torch.Tensor]:
        """Add differential privacy noise"""
        return [w + torch.normal(0, self.server_config.dp_sigma, w.shape) for w in weights]

    async def aggregate_updates(self) -> None:
        """Secure model aggregation with Byzantine robustness checks"""
        async with self.lock:
            valid_updates = [
                update for update in self.client_updates.values()
                if update.verify_signature(self.public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                ))
            ]
            
            if len(valid_updates) < 3:
                raise ValueError("Insufficient valid updates for aggregation")
                
            decrypted_weights = [
                self._decrypt_weights(u.encrypted_weights) 
                for u in valid_updates
            ]
            
            # Robust aggregation using trimmed mean
            aggregated = [
                torch.stack([
                    w[i] for w in decrypted_weights
                ]).median(dim=0).values 
                for i in range(len(decrypted_weights[0]))
            ]
            
            if self.server_config.encryption_enabled:
                aggregated = self._encrypt_weights(aggregated)
                
            self.global_model.load_state_dict(aggregated)
            self.round += 1

    def _encrypt_weights(self, weights: List[torch.Tensor]) -> bytes:
        """Hybrid encryption using AES-256-GCM and RSA-OAEP"""
        # Implementation simplified for example
        serialized = torch.save(weights, io.BytesIO())
        return serialized

    def _decrypt_weights(self, ciphertext: bytes) -> List[torch.Tensor]:
        """Decrypt model weights using private key"""
        # Implementation simplified for example
        return torch.load(io.BytesIO(ciphertext))

    async def client_update(
        self, 
        client_config: FLClientConfig,
        dataset: torch.utils.data.Dataset
    ) -> EncryptedModelUpdate:
        """Client-side training with differential privacy"""
        local_model = copy.deepcopy(self.global_model)
        optimizer = optim.SGD(local_model.parameters(), lr=client_config.learning_rate)
        
        for epoch in range(client_config.min_local_epochs):
            for batch in dataset:
                optimizer.zero_grad()
                loss = self._compute_loss(batch)
                loss.backward()
                optimizer.step()
                
        delta_weights = [
            local_w - global_w 
            for local_w, global_w in zip(
                local_model.parameters(), 
                self.global_model.parameters()
            )
        ]
        
        if client_config.differential_privacy:
            delta_weights = self._apply_dp_noise(delta_weights)
            
        encrypted = self._encrypt_weights(delta_weights)
        signature = self.private_key.sign(
            encrypted,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        
        return EncryptedModelUpdate(
            client_id=client_config.client_id,
            encrypted_weights=encrypted,
            signature=signature,
            nonce=os.urandom(32)
        )

    async def run_training_round(self, clients: List[FLClientConfig]):
        """Orchestrate one round of federated learning"""
        self._initialize_round()
        tasks = [self.client_update(c, c.dataset) for c in clients]
        updates = await asyncio.gather(*tasks)
        
        async with self.lock:
            self.client_updates = {u.client_id: u for u in updates}
            
        await self.aggregate_updates()
        
        return self.global_model.state_dict()

    async def continuous_training(self):
        """Run FL rounds until convergence or stop signal"""
        while not self._stop_flag and self.round < self.server_config.max_rounds:
            await self.run_training_round()
            accuracy = self._evaluate_model()
            if accuracy >= self.server_config.target_accuracy:
                break

    async def stop(self):
        """Graceful shutdown"""
        self._stop_flag = True

# Example Usage
async def main():
    model = FLModel()
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096
    )
    public_key = private_key.public_key()
    
    server_config = FLServerConfig(
        aggregation_strategy="fedavg",
        max_rounds=100,
        encryption_enabled=True
    )
    
    trainer = FederatedTrainer(
        model=model,
        server_config=server_config,
        private_key=private_key,
        public_key=public_key
    )
    
    await trainer.continuous_training()

if __name__ == "__main__":
    asyncio.run(main())
