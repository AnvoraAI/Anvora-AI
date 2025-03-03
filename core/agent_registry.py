# agents/core/agent_registry.py

import threading
import logging
import time
from typing import Dict, List, Optional, Set, Tuple, Type, Callable
from uuid import uuid4
from pydantic import BaseModel, Field
from datetime import datetime

# Custom Types
AgentID = str
AgentType = str
AgentStatus = str

class AgentMetadata(BaseModel):
    capabilities: Set[str] = Field(default_factory=set)
    version: str = "1.0.0"
    supported_protocols: List[str] = Field(default_factory=list)
    resource_usage: Dict[str, float] = Field(default_factory=dict)

class AgentRecord(BaseModel):
    agent_id: AgentID
    agent_type: AgentType
    status: AgentStatus = "INACTIVE"
    metadata: AgentMetadata
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    endpoint: Optional[str] = None
    load_balancing_weight: float = 1.0

class AgentRegistry:
    def __init__(self):
        self._registry: Dict[AgentID, AgentRecord] = {}
        self._type_index: Dict[AgentType, Set[AgentID]] = {}
        self._lock = threading.RLock()
        self.logger = logging.getLogger("AgentRegistry")
        self._health_check_interval = 30  # seconds
        
    def register_agent(
        self,
        agent_type: AgentType,
        metadata: AgentMetadata,
        endpoint: Optional[str] = None
    ) -> AgentID:
        """
        Register a new agent with automatic ID generation
        
        Args:
            agent_type: Category of agent (e.g., "SECURITY", "TRADING")
            metadata: Capabilities and version info
            endpoint: Network location if applicable
            
        Returns:
            Generated unique agent ID
        """
        with self._lock:
            agent_id = f"{agent_type}-{uuid4().hex[:8]}"
            record = AgentRecord(
                agent_id=agent_id,
                agent_type=agent_type,
                metadata=metadata,
                endpoint=endpoint,
                status="HEALTHY"
            )
            
            self._registry[agent_id] = record
            self._type_index.setdefault(agent_type, set()).add(agent_id)
            
            self.logger.info(f"Registered {agent_type} agent: {agent_id}")
            return agent_id

    def unregister_agent(self, agent_id: AgentID) -> None:
        """Remove agent from registry"""
        with self._lock:
            if agent_id not in self._registry:
                raise KeyError(f"Agent {agent_id} not found")
                
            agent_type = self._registry[agent_id].agent_type
            self._type_index[agent_type].remove(agent_id)
            del self._registry[agent_id]
            
            self.logger.info(f"Unregistered agent: {agent_id}")

    def update_heartbeat(self, agent_id: AgentID) -> None:
        """Record latest activity timestamp"""
        with self._lock:
            if agent_id not in self._registry:
                raise KeyError(f"Agent {agent_id} not found")
                
            self._registry[agent_id].last_heartbeat = datetime.utcnow()

    def find_agents(
        self,
        agent_type: Optional[AgentType] = None,
        status: Optional[AgentStatus] = None,
        capability: Optional[str] = None
    ) -> List[AgentRecord]:
        """
        Discover agents matching criteria
        
        Args:
            agent_type: Filter by agent category
            status: Filter by current status
            capability: Required metadata capability
            
        Returns:
            List of matching agent records
        """
        with self._lock:
            candidates = self._registry.values()
            
            if agent_type:
                candidates = [r for r in candidates if r.agent_type == agent_type]
            if status:
                candidates = [r for r in candidates if r.status == status]
            if capability:
                candidates = [
                    r for r in candidates 
                    if capability in r.metadata.capabilities
                ]
                
            return candidates

    def get_agent(self, agent_id: AgentID) -> AgentRecord:
        """Retrieve full agent record"""
        with self._lock:
            return self._registry[agent_id]

    def set_agent_status(self, agent_id: AgentID, status: AgentStatus) -> None:
        """Update operational status"""
        with self._lock:
            if agent_id not in self._registry:
                raise KeyError(f"Agent {agent_id} not found")
                
            self._registry[agent_id].status = status
            self.logger.debug(f"Updated {agent_id} status to {status}")

    def start_health_monitoring(self) -> None:
        """Initialize background health checks"""
        def monitor():
            while True:
                self._perform_health_checks()
                time.sleep(self._health_check_interval)
                
        threading.Thread(target=monitor, daemon=True).start()

    def _perform_health_checks(self) -> None:
        """Validate agent health status"""
        with self._lock:
            for agent_id, record in self._registry.items():
                try:
                    # Placeholder for actual health check implementation
                    is_healthy = self._check_agent_health(record)
                    new_status = "HEALTHY" if is_healthy else "UNHEALTHY"
                    
                    if record.status != new_status:
                        self.set_agent_status(agent_id, new_status)
                        self.logger.warning(
                            f"Agent {agent_id} status changed to {new_status}"
                        )
                        
                except Exception as e:
                    self.logger.error(f"Health check failed for {agent_id}: {str(e)}")
                    self.set_agent_status(agent_id, "UNKNOWN")

    def _check_agent_health(self, record: AgentRecord) -> bool:
        """Customizable health check logic"""
        # Implement protocol-specific checks (gRPC/HTTP ping, resource usage, etc.)
        return (
            datetime.utcnow() - record.last_heartbeat
        ).total_seconds() < self._health_check_interval * 2

    def load_balance_select(
        self,
        agent_type: AgentType,
        strategy: str = "ROUND_ROBIN"
    ) -> Optional[AgentID]:
        """
        Select agent based on load balancing strategy
        
        Args:
            agent_type: Target agent category
            strategy: Selection algorithm (ROUND_ROBIN/RANDOM/WEIGHTED)
            
        Returns:
            Selected agent ID or None
        """
        with self._lock:
            candidates = [
                r for r in self.find_agents(agent_type=agent_type, status="HEALTHY")
                if "LOAD_BALANCED" in r.metadata.capabilities
            ]
            
            if not candidates:
                return None
                
            if strategy == "ROUND_ROBIN":
                return self._round_robin_select(agent_type, candidates)
            elif strategy == "RANDOM":
                return random.choice(candidates).agent_id
            elif strategy == "WEIGHTED":
                return self._weighted_select(candidates)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")

    def _round_robin_select(self, agent_type: AgentType, candidates: List[AgentRecord]) -> AgentID:
        """Stateful round-robin selection"""
        # Implementation requires persistent counter (omitted for brevity)
        pass

    def _weighted_select(self, candidates: List[AgentRecord]) -> AgentID:
        """Weighted random selection"""
        total = sum(r.load_balancing_weight for r in candidates)
        selection = random.uniform(0, total)
        return next(
            r.agent_id for r in candidates
            if (selection := selection - r.load_balancing_weight) <= 0
        )
