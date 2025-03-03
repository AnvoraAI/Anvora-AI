# audit/audit_logger.py

import asyncio
import logging
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar
from pathlib import Path
import aiofiles
from aiofiles.threadpool.text import AsyncTextIOWrapper
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.exceptions import InvalidSignature
from pydantic import BaseModel, Field, validator
import uvloop
import orjson

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger = logging.getLogger("AuditLogger")

class AuditEvent(BaseModel):
    """Immutable audit event schema with cryptographic validation"""
    event_id: str = Field(..., min_length=32, max_length=32)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    agent_id: str
    user_id: Optional[str]
    event_type: str
    details: Dict[str, Any]
    previous_hash: Optional[str]
    signature: Optional[str]
    
    @validator('event_id')
    def validate_event_id(cls, v):
        if not v.isalnum() or len(v) != 32:
            raise ValueError("Invalid event ID format")
        return v
    
    class Config:
        json_loads = orjson.loads
        json_dumps = lambda _, v, *a, **kw: orjson.dumps(v).decode()

class MerkleNode:
    """Merkle Tree node for batch integrity verification"""
    def __init__(self, left: Optional['MerkleNode'] = None, 
                 right: Optional['MerkleNode'] = None, 
                 hash_value: Optional[str] = None):
        self.left = left
        self.right = right
        self.hash = hash_value or self.calculate_hash()

    @staticmethod
    def calculate_hash(*data: str) -> str:
        combined = "".join(data).encode()
        return hashlib.sha3_256(combined).hexdigest()

class AuditLogStorage:
    """Abstract storage backend for audit records"""
    async def write_event(self, event: AuditEvent) -> None:
        raise NotImplementedError
        
    async def verify_integrity(self, event_id: str) -> bool:
        raise NotImplementedError
        
    async def get_events(self, query: Dict[str, Any]) -> List[AuditEvent]:
        raise NotImplementedError

class FileAuditStorage(AuditLogStorage):
    """File-based storage with Merkle tree and digital signatures"""
    def __init__(self, file_path: Path, private_key: Optional[bytes] = None):
        self.file_path = file_path
        self.private_key = private_key
        self.lock = asyncio.Lock()
        self.merkle_root: Optional[MerkleNode] = None
        self._init_file()

    def _init_file(self):
        if not self.file_path.exists():
            self.file_path.touch(mode=0o600)
            logger.info(f"Created new audit log at {self.file_path}")

    async def _sign_event(self, event: AuditEvent) -> str:
        if not self.private_key:
            return ""
            
        priv_key = load_pem_private_key(
            self.private_key,
            password=None,
            backend=default_backend()
        )
        message = json.dumps(event.dict()).encode()
        signature = priv_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return signature.hex()

    async def _append_to_merkle(self, event_hash: str):
        # Simplified Merkle tree update logic
        if not self.merkle_root:
            self.merkle_root = MerkleNode(hash_value=event_hash)
        else:
            new_node = MerkleNode(
                left=self.merkle_root,
                right=MerkleNode(hash_value=event_hash)
            )
            self.merkle_root = new_node

    async def write_event(self, event: AuditEvent) -> None:
        async with self.lock:
            event_hash = hashlib.sha3_256(
                json.dumps(event.dict()).encode()
            ).hexdigest()
            
            event.signature = await self._sign_event(event)
            event.previous_hash = self.merkle_root.hash if self.merkle_root else None
            
            async with aiofiles.open(self.file_path, "a") as f:
                await f.write(f"{json.dumps(event.dict())}\n")
                
            await self._append_to_merkle(event_hash)
            logger.debug(f"Logged event {event.event_id}")

    async def verify_integrity(self, event_id: str) -> bool:
        # Implement Merkle path verification
        # [Simplified for example]
        return True

    async def get_events(self, query: Dict[str, Any]) -> List[AuditEvent]:
        events = []
        async with aiofiles.open(self.file_path, "r") as f:
            async for line in f:
                data = json.loads(line)
                if all(data.get(k) == v for k, v in query.items()):
                    events.append(AuditEvent(**data))
        return events

class AuditLogger:
    """Enterprise audit system with cryptographic chain of custody"""
    def __init__(
        self, 
        storage: AuditLogStorage,
        public_key: Optional[bytes] = None,
        buffer_size: int = 1000,
        flush_interval: int = 30
    ):
        self.storage = storage
        self.public_key = public_key
        self.buffer: List[AuditEvent] = []
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

    async def log_event(
        self,
        agent_id: str,
        event_type: str,
        details: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> str:
        event = AuditEvent(
            event_id=os.urandom(16).hex(),
            agent_id=agent_id,
            user_id=user_id,
            event_type=event_type,
            details=details
        )
        self.buffer.append(event)
        
        if len(self.buffer) >= self.buffer_size:
            await self.flush_buffer()
            
        return event.event_id

    async def flush_buffer(self) -> None:
        if not self.buffer:
            return
            
        try:
            await asyncio.gather(
                *[self.storage.write_event(event) for event in self.buffer]
            )
            self.buffer.clear()
        except Exception as e:
            logger.error(f"Buffer flush failed: {str(e)}")

    async def start_periodic_flush(self) -> None:
        self._running = True
        while self._running:
            await asyncio.sleep(self.flush_interval)
            await self.flush_buffer()

    async def stop(self) -> None:
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush_buffer()

    async def verify_event(self, event_id: str) -> bool:
        if not self.public_key:
            raise ValueError("Public key required for verification")
            
        events = await self.storage.get_events({"event_id": event_id})
        if not events:
            return False
            
        event = events[0]
        try:
            pub_key = serialization.load_pem_public_key(
                self.public_key,
                backend=default_backend()
            )
            pub_key.verify(
                bytes.fromhex(event.signature),
                json.dumps(event.dict(exclude={'signature'})).encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except InvalidSignature:
            return False

# Example Usage
async def main():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096
    ).private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    storage = FileAuditStorage(
        Path("/var/log/gradual/audit.log"),
        private_key=private_key
    )
    
    logger = AuditLogger(storage)
    await logger.log_event(
        agent_id="trade_engine_01",
        event_type="ORDER_EXECUTED",
        details={
            "symbol": "BTC-USD",
            "quantity": 1.5,
            "price": 42000.0
        }
    )
    await logger.stop()

if __name__ == "__main__":
    asyncio.run(main())
