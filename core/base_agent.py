# agents/core/base_agent.py

from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, Optional, TypeVar
from pydantic import BaseModel

ConfigT = TypeVar("ConfigT", bound=BaseModel)

class AgentConfigurationError(Exception):
    """Base exception for agent configuration failures"""
    pass

class AgentInitializationError(Exception):
    """Critical failure during agent initialization"""
    pass

class BaseAgent(ABC):
    def __init__(
        self, 
        agent_id: str,
        config: ConfigT,
        logger: Optional[logging.Logger] = None
    ):
        """
        Abstract base class for all agents
        
        :param agent_id: Unique identifier for the agent instance
        :param config: Agent-specific configuration (Pydantic model)
        :param logger: Optional pre-configured logger instance
        """
        if not agent_id:
            raise AgentConfigurationError("agent_id cannot be empty")
        
        self._agent_id = agent_id
        self._config = config
        self._logger = logger or self._configure_logger()
        
        self._validate_config()
        
    def _configure_logger(self) -> logging.Logger:
        logger = logging.getLogger(f"agents.{self._agent_id}")
        logger.setLevel(logging.INFO)
        return logger

    def _validate_config(self) -> None:
        """Validate configuration using Pydantic model"""
        if not isinstance(self._config, BaseModel):
            raise AgentConfigurationError(
                f"Config must be a Pydantic model, got {type(self._config)}"
            )
        try:
            self._config.model_validate(self._config.model_dump())
        except Exception as e:
            raise AgentConfigurationError(
                f"Invalid config for agent {self._agent_id}: {str(e)}"
            ) from e

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def config(self) -> ConfigT:
        return self._config

    @abstractmethod
    async def start(self) -> None:
        """Start the agent's main execution loop"""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully terminate the agent"""
        pass

    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        Return agent status details
        Format: {
            "status": "running|degraded|stopped",
            "last_heartbeat": timestamp,
            "metrics": {...}
        }
        """
        pass

    def receive_message(self, message: Dict[str, Any]) -> None:
        """
        Handle incoming messages from other agents
        Implementations should override for specific message types
        """
        self._logger.debug(f"Received message: {message}")
        # Placeholder for message validation/processing pipeline
        self._validate_message_schema(message)

    def _validate_message_schema(self, message: Dict[str, Any]) -> bool:
        """Basic message schema validation (override for protocol-specific checks)"""
        required_keys = {"sender_id", "message_type", "payload"}
        if not required_keys.issubset(message.keys()):
            self._logger.error(f"Invalid message format: missing required keys")
            return False
        return True

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self._agent_id}>"
