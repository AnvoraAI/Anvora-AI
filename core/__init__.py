# gradual-agent/__init__.py

__version__ = "0.1.0"
__all__ = ["BaseAgent", "RayCluster", "ZKPProver", "Settings", "load_config"]

import logging
from typing import Type

# Core components
from .agents.core.base_agent import BaseAgent
from .agents.security.zkp_prover import ZKPProver
from .services.orchestration.ray_cluster import RayCluster

# Configuration
from .config import Settings, load_config

# Initialize default logger
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

# Type aliases
AgentType = Type[BaseAgent]
ProofProtocol = Type[ZKPProver]

# Runtime validation
try:
    import ray  # noqa
    import cryptography  # noqa
except ImportError as e:
    raise RuntimeError("Missing critical dependencies: " + str(e)) from e
