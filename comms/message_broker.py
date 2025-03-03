# comms/message_broker.py

import logging
import asyncio
import json
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any, AsyncGenerator, Dict, List, Optional, Tuple, Type, TypeVar, Union
)
import uuid
from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from aiormq import AMQPConnectionError, ChannelNotFoundEntityError
from ray.util.annotations import PublicAPI
from cryptography.fernet import Fernet

logger = logging.getLogger("MessageBroker")

# ----------------- Core Data Models -----------------

class MessageMetadata(BaseModel):
    message_id: str = uuid.uuid4().hex
    producer_id: str
    timestamp: float
    ttl: Optional[float] = None  # Seconds until expiration
    priority: int = 0
    retry_count: int = 0
    encryption_key_id: Optional[str] = None

class BrokerMessage(BaseModel):
    metadata: MessageMetadata
    payload: Union[Dict[str, Any], bytes]
    routing_key: str
    headers: Dict[str, str] = {}

# ----------------- Configuration -----------------

@dataclass
class BrokerConfig:
    host: str = "localhost"
    port: int = 5672  # AMQP default
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    exchange: str = "gradual_agent"
    queue_prefix: str = "agent_"
    max_retries: int = 3
    heartbeat: int = 60
    ssl: bool = False
    encryption_key: Optional[str] = None

# ----------------- Exceptions -----------------

class MessageBrokerError(Exception):
    """Base exception for all broker-related errors"""

class SerializationError(MessageBrokerError):
    """Failed to serialize/deserialize message"""

class DeliveryFailedError(MessageBrokerError):
    """Message delivery failed after retries"""

class SecurityError(MessageBrokerError):
    """Encryption/decryption or validation failure"""

# ----------------- Base Broker Interface -----------------

class MessageBroker(ABC):
    def __init__(self, config: BrokerConfig):
        self.config = config
        self._fernet = (
            Fernet(config.encryption_key.encode()) 
            if config.encryption_key else None
        )
        self._connected = False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to message broker"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection gracefully"""
        pass

    @abstractmethod
    async def publish(
        self,
        message: BrokerMessage,
        routing_key: Optional[str] = None
    ) -> str:
        """Publish message to broker with delivery confirmation"""
        pass

    @abstractmethod
    @asynccontextmanager
    async def subscribe(
        self,
        queue: str,
        routing_keys: List[str]
    ) -> AsyncGenerator[AsyncGenerator[BrokerMessage, None], None]:
        """Context manager for consuming messages"""
        yield  # Type hinting workaround

    # ----------------- Security Layer -----------------

    def _encrypt_payload(self, payload: bytes) -> bytes:
        if not self._fernet:
            return payload
        return self._fernet.encrypt(payload)

    def _decrypt_payload(self, token: bytes) -> bytes:
        if not self._fernet:
            return token
        return self._fernet.decrypt(token)

    # ----------------- Serialization -----------------

    def _serialize_message(self, message: BrokerMessage) -> bytes:
        try:
            payload = (
                json.dumps(message.payload).encode()
                if isinstance(message.payload, dict)
                else message.payload
            )
            encrypted = self._encrypt_payload(payload)
            return json.dumps({
                "metadata": message.metadata.dict(),
                "payload": encrypted.decode(),
                "routing_key": message.routing_key,
                "headers": message.headers
            }).encode()
        except (TypeError, ValidationError, ValueError) as e:
            raise SerializationError(f"Serialization failed: {str(e)}") from e

    def _deserialize_message(self, data: bytes) -> BrokerMessage:
        try:
            raw = json.loads(data.decode())
            decrypted = self._decrypt_payload(raw["payload"].encode())
            payload = (
                json.loads(decrypted)
                if raw["headers"].get("content_type") == "application/json"
                else decrypted
            )
            return BrokerMessage(
                metadata=MessageMetadata(**raw["metadata"]),
                payload=payload,
                routing_key=raw["routing_key"],
                headers=raw["headers"]
            )
        except (json.JSONDecodeError, KeyError, ValidationError) as e:
            raise SerializationError(f"Deserialization failed: {str(e)}") from e

    # ----------------- Retry Decorator -----------------

    async def _retry_on_failure(self, coro, max_retries: int = None):
        max_retries = max_retries or self.config.max_retries
        for attempt in range(max_retries + 1):
            try:
                return await coro
            except (AMQPConnectionError, ChannelNotFoundEntityError) as e:
                if attempt == max_retries:
                    raise DeliveryFailedError(
                        f"Operation failed after {max_retries} retries"
                    ) from e
                backoff = 2 ** attempt
                logger.warning(f"Retrying in {backoff}s (attempt {attempt+1})")
                await asyncio.sleep(backoff)
                await self.connect()

# ----------------- Redis Implementation -----------------

class RedisBroker(MessageBroker):
    def __init__(self, config: BrokerConfig):
        super().__init__(config)
        self._redis: Optional[Redis] = None
        self._pubsub: Optional[Redis.pubsub] = None

    async def connect(self) -> None:
        if not self._connected:
            self._redis = Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                ssl=self.config.ssl,
                decode_responses=False
            )
            await self._redis.ping()
            self._connected = True

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()
            self._connected = False

    async def publish(
        self, 
        message: BrokerMessage,
        routing_key: Optional[str] = None
    ) -> str:
        await self._retry_on_failure(self._redis.publish)(
            routing_key or self.config.exchange,
            self._serialize_message(message)
        )
        return message.metadata.message_id

    @asynccontextmanager
    async def subscribe(
        self, 
        queue: str,
        routing_keys: List[str]
    ) -> AsyncGenerator[AsyncGenerator[BrokerMessage, None], None]:
        if not self._pubsub:
            self._pubsub = self._redis.pubsub()

        await self._pubsub.subscribe(*routing_keys)
        try:
            async def message_generator():
                while True:
                    raw = await self._pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )
                    if raw:
                        yield self._deserialize_message(raw["data"])
            yield message_generator()
        finally:
            await self._pubsub.unsubscribe(*routing_keys)

# ----------------- AMQP/RabbitMQ Implementation -----------------

class AMQPBroker(MessageBroker):
    def __init__(self, config: BrokerConfig):
        super().__init__(config)
        self._connection = None
        self._channel = None

    async def connect(self) -> None:
        from aio_pika import connect_robust, ExchangeType
        if not self._connected:
            self._connection = await connect_robust(
                host=self.config.host,
                port=self.config.port,
                login=self.config.username,
                password=self.config.password,
                virtualhost=self.config.virtual_host,
                ssl=self.config.ssl,
                heartbeat=self.config.heartbeat
            )
            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                name=self.config.exchange,
                type=ExchangeType.TOPIC,
                durable=True
            )
            self._connected = True

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connected = False

    async def publish(
        self,
        message: BrokerMessage,
        routing_key: Optional[str] = None
    ) -> str:
        rk = routing_key or message.routing_key
        msg = self._serialize_message(message)
        await self._retry_on_failure(self._exchange.publish)(
            message=msg,
            routing_key=rk,
            headers=message.headers,
            expiration=message.metadata.ttl
        )
        return message.metadata.message_id

    @asynccontextmanager
    async def subscribe(
        self,
        queue: str,
        routing_keys: List[str]
    ) -> AsyncGenerator[AsyncGenerator[BrokerMessage, None], None]:
        from aio_pika import IncomingMessage
        q = await self._channel.declare_queue(
            name=f"{self.config.queue_prefix}{queue}",
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-message-ttl": 86400000  # 24h in ms
            }
        )
        await q.bind(self._exchange, routing_keys=routing_keys)
        
        async with q.iterator() as queue_iter:
            async def message_generator():
                async for raw in queue_iter:
                    async with raw.process():
                        yield self._deserialize_message(raw.body)
            yield message_generator()

# ----------------- Ray Actor-Based Broker -----------------

@PublicAPI
class RayBroker(MessageBroker):
    """Distributed broker using Ray's internal pubsub"""
    def __init__(self, config: BrokerConfig):
        super().__init__(config)
        from ray.util import get_node_ip_address
        self.node_ip = get_node_ip_address()
        self._subscribers = {}

    async def connect(self) -> None:
        import ray
        if not ray.is_initialized():
            ray.init(address="
