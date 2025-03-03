# aggregation/model_aggregator.py

import logging
import asyncio
import hashlib
import copy
from abc import ABC, abstractmethod
from collections import defaultdict
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
import uvloop
from ray.util import ActorPool

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger = logging.getLogger("ModelAggregator")

class ModelUpdate(BaseModel):
    client_id: str = Field(..., min_length=16)
    weights: List[torch.Tensor]
    metadata: Dict[str, float] = Field(default_factory=dict)
    signature: bytes = Field(..., min_length=256)
    timestamp: datetime = Field(default_factory=lambda: datetime.utcnow())
    nonce: bytes = Field(..., min_length=32)
    protocol_version: str = "1.2"

    def validate_checksum(self) -> bool:
        data = b"".join(
            w.cpu().numpy().tobytes() 
            for w in self.weights
        ) + self.nonce
        return hashlib.sha3_256(data).hexdigest() == self.metadata.get("checksum")

class AggregationStrategy(ABC):
    @abstractmethod
    async def aggregate(
        self, 
        updates: List[ModelUpdate],
        global_weights: List[torch.Tensor]
    ) -> List[torch.Tensor]:
        pass

class FedAvgStrategy(AggregationStrategy):
    async def aggregate(self, updates, global_weights):
        sample_counts = [u.metadata["sample_count"] for u in updates]
        total_samples = sum(sample_counts)
        
        weighted_weights = [
            [w * sc for w in u.weights] 
            for u, sc in zip(updates, sample_counts)
        ]
        
        return [
            sum(w[i] for w in weighted_weights) / total_samples
            for i in range(len(weighted_weights[0]))
        ]

class FedProxStrategy(AggregationStrategy):
    def __init__(self, mu: float = 0.01):
        self.mu = mu
        
    async def aggregate(self, updates, global_weights):
        avg_weights = await FedAvgStrategy().aggregate(updates, global_weights)
        return [
            avg_w + self.mu * (avg_w - global_w)
            for avg_w, global_w in zip(avg_weights, global_weights)
        ]

class ByzantineRobustAggregator(AggregationStrategy):
    async def aggregate(self, updates, global_weights):
        # Trimmed mean aggregation
        layer_wise_updates = defaultdict(list)
        for u in updates:
            for i, w in enumerate(u.weights):
                layer_wise_updates[i].append(w)
                
        return [
            torch.stack(layer_updates).median(dim=0).values
            for i, layer_updates in layer_wise_updates.items()
        ]

class EncryptedAggregationWrapper(AggregationStrategy):
    def __init__(self, strategy: AggregationStrategy, public_key: bytes):
        self.strategy = strategy
        self.public_key = load_pem_public_key(public_key)
        
    async def aggregate(self, updates, global_weights):
        valid_updates = []
        for u in updates:
            try:
                self.public_key.verify(
                    u.signature,
                    b"".join(w.cpu().numpy().tobytes() for w in u.weights),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                valid_updates.append(u)
            except InvalidSignature:
                logger.warning(f"Invalid signature from {u.client_id}")
                
        return await self.strategy.aggregate(valid_updates, global_weights)

class ModelAggregator:
    def __init__(
        self,
        strategy: AggregationStrategy,
        validation_threshold: float = 0.8,
        ray_pool: Optional[ActorPool] = None
    ):
        self.strategy = strategy
        self.validation_threshold = validation_threshold
        self.ray_pool = ray_pool
        self.lock = asyncio.Lock()
        
    async def parallel_aggregate(
        self,
        updates: List[ModelUpdate],
        global_weights: List[torch.Tensor]
    ) -> List[torch.Tensor]:
        if self.ray_pool is None:
            return await self.strategy.aggregate(updates, global_weights)
            
        weights_ref = [w.cpu().numpy() for w in global_weights]
        results = await asyncio.gather(*[
            self.ray_pool.submit(
                lambda actor, u: actor.aggregate_batch.remote(u, weights_ref),
                update
            )
            for update in updates
        ])
        return [torch.from_numpy(w).to(global_weights[0].device) for w in results[0]]

    async def validate_updates(
        self,
        updates: List[ModelUpdate],
        previous_weights: List[torch.Tensor]
    ) -> List[ModelUpdate]:
        valid_updates = []
        async with self.lock:
            for u in updates:
                if not u.validate_checksum():
                    logger.error(f"Checksum failed for {u.client_id}")
                    continue
                if datetime.utcnow() - u.timestamp > timedelta(minutes=5):
                    logger.warning(f"Stale update from {u.client_id}")
                    continue
                valid_updates.append(u)
                
            validity_ratio = len(valid_updates) / len(updates)
            if validity_ratio < self.validation_threshold:
                raise ValueError("Insufficient valid updates for consensus")
                
        return valid_updates

    async def federated_averaging(
        self,
        updates: List[ModelUpdate],
        global_weights: List[torch.Tensor]
    ) -> List[torch.Tensor]:
        valid_updates = await self.validate_updates(updates, global_weights)
        return await self.strategy.aggregate(valid_updates, global_weights)

    async def run_aggregation_round(
        self,
        updates: List[ModelUpdate],
        global_weights: List[torch.Tensor],
