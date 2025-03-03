# scheduler/task_scheduler.py

import logging
import asyncio
import time
import json
import heapq
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Tuple, TypeVar, Union
)
from uuid import uuid4
from cryptography.fernet import Fernet
from pydantic import BaseModel, ValidationError
import numpy as np
import ray
from ray import ObjectRef
from ray.util import metrics
from .crypto_utils import validate_zk_proof
from .message_broker import MessageBroker

logger = logging.getLogger("TaskScheduler")
metrics.set_default_tags({"service": "task_scheduler"})

# ----------------- Data Models -----------------

class TaskRequest(BaseModel):
    task_id: str = field(default_factory=lambda: uuid4().hex)
    payload: Union[Dict[str, Any], bytes]
    priority: int = 0  # Higher values = higher priority
    deadline: Optional[float] = None  # Unix timestamp
    zk_proof: Optional[str] = None  # Zero-knowledge proof
    resource_requirements: Dict[str, float] = {}  # e.g., {"CPU": 2.0, "GPU": 1.0}

class TaskResult(BaseModel):
    task_id: str
    status: str  # "PENDING", "RUNNING", "COMPLETED", "FAILED", "TIMEOUT"
    result: Optional[Any] = None
    metrics: Dict[str, float] = {}  # e.g., {"latency": 0.5, "cpu_usage": 80.0}
    node_id: Optional[str] = None

class NodeResources(BaseModel):
    node_id: str
    available: Dict[str, float]  # Available resources
    total: Dict[str, float]     # Total resources
    labels: Dict[str, str]      # e.g., {"zone": "us-east1", "gpu_type": "A100"}

# ----------------- Exceptions -----------------

class SchedulerError(Exception):
    """Base scheduler exception"""

class ResourceUnavailableError(SchedulerError):
    """No nodes meet resource requirements"""

class SecurityViolationError(SchedulerError):
    """Task failed security checks"""

class DeadlineExceededError(SchedulerError):
    """Task missed deadline"""

# ----------------- Core Scheduler -----------------

class TaskScheduler:
    def __init__(
        self,
        message_broker: MessageBroker,
        encryption_key: Optional[bytes] = None,
        max_retries: int = 3,
        concurrency_limit: int = 1000
    ):
        self.broker = message_broker
        self.fernet = Fernet(encryption_key) if encryption_key else None
        self.max_retries = max_retries
        self.concurrency_sem = asyncio.Semaphore(concurrency_limit)
        
        # State management
        self.task_queue: List[Tuple[int, float, TaskRequest]] = []  # Priority heap
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.node_resources: Dict[str, NodeResources] = {}
        self.task_retries: Dict[str, int] = defaultdict(int)
        self.completed_tasks = asyncio.Queue()
        
        # Metrics
        self.metrics = {
            "scheduled_tasks": metrics.Counter("scheduled_tasks_total"),
            "failed_tasks": metrics.Counter("failed_tasks_total"),
            "current_concurrency": metrics.Gauge("current_concurrency"),
        }

    # ----------------- Public API -----------------

    async def schedule_task(self, request: TaskRequest) -> str:
        """Main entry point for task submission"""
        self._validate_task(request)
        await self._enqueue_task(request)
        return request.task_id

    async def get_result(self, task_id: str, timeout: float = 30.0) -> TaskResult:
        """Retrieve task result with timeout"""
        return await asyncio.wait_for(
            self._poll_result(task_id),
            timeout=timeout
        )

    def register_node(self, node: NodeResources):
        """Update available cluster resources"""
        self.node_resources[node.node_id] = node

    # ----------------- Core Scheduling Logic -----------------

    async def _enqueue_task(self, request: TaskRequest):
        """Add task to priority queue with security checks"""
        if self.fernet:
            encrypted = self.fernet.encrypt(json.dumps(request.payload).encode())
            request.payload = encrypted

        # Priority heap ordered by (priority, deadline)
        heapq.heappush(
            self.task_queue,
            (-request.priority, request.deadline or float('inf'), request)
        )
        self.metrics["scheduled_tasks"].inc()

    async def run_scheduler_loop(self):
        """Main scheduling loop (run as background task)"""
        while True:
            if self.task_queue and not self.concurrency_sem.locked():
                async with self.concurrency_sem:
                    _, _, request = heapq.heappop(self.task_queue)
                    task = asyncio.create_task(
                        self._execute_task(request),
                        name=f"Task-{request.task_id}"
                    )
                    self.running_tasks[request.task_id] = task
            await asyncio.sleep(0.01)

    async def _execute_task(self, request: TaskRequest) -> TaskResult:
        """Execute task with resource allocation and retries"""
        node_id = self._allocate_resources(request)
        result = TaskResult(task_id=request.task_id, status="RUNNING", node_id=node_id)
        
        try:
            task_ref = await self._submit_to_ray(request, node_id)
            raw_result = await task_ref
            result.status = "COMPLETED"
            result.result = raw_result.get("result")
            result.metrics = raw_result.get("metrics", {})
        except Exception as e:
            result.status = "FAILED"
            result.metrics["error"] = str(e)
            self._handle_task_failure(request, e)
        finally:
            self._release_resources(node_id, request.resource_requirements)
            await self.completed_tasks.put(result)
            self.running_tasks.pop(request.task_id, None)
            self._update_metrics(result)
            
        return result

    # ----------------- Resource Management -----------------

    def _allocate_resources(self, request: TaskRequest) -> str:
        """Find suitable node and allocate resources"""
        for node_id, node in self.node_resources.items():
            if all(
                node.available.get(res, 0) >= need
                for res, need in request.resource_requirements.items()
            ):
                # Allocate resources
                for res, need in request.resource_requirements.items():
                    self.node_resources[node_id].available[res] -= need
                return node_id
        raise ResourceUnavailableError(
            f"No nodes meet requirements: {request.resource_requirements}"
        )

    def _release_resources(self, node_id: str, resources: Dict[str, float]):
        """Return resources to node pool"""
        for res, need in resources.items():
            self.node_resources[node_id].available[res] += need

    # ----------------- Ray Integration -----------------

    async def _submit_to_ray(self, request: TaskRequest, node_id: str) -> ObjectRef:
        """Submit task to Ray cluster with node affinity"""
        # Decrypt payload before processing
        if self.fernet:
            decrypted = self.fernet.decrypt(request.payload)
            request.payload = json.loads(decrypted)
        
        # Validate ZKP proof
        if not validate_zk_proof(request.zk_proof):
            raise SecurityViolationError("Invalid ZKP proof for task")

        # Submit to Ray with node affinity
        options = {
            "resources": {f"node:{node_id}": 0.1},
            "max_retries": self.max_retries
        }
        return await ray.remote(
            self._wrap_task_execution(request)
        ).options(**options).remote()

    def _wrap_task_execution(self, request: TaskRequest) -> Callable:
        """Create Ray task with deadline enforcement"""
        async def _ray_task_wrapper():
            start_time = time.time()
            result = {"result": None, "metrics": {}}
            
            try:
                # Actual task processing
                result["result"] = await self._process_task(request)
                result["metrics"]["processing_time"] = time.time() - start_time
            except asyncio.CancelledError:
                result["metrics"]["cancelled"] = True
                raise
                
            return result
            
        return _ray_task_wrapper

    # ----------------- Security & Validation -----------------

    def _validate_task(self, request: TaskRequest):
        """Perform security checks before scheduling"""
        if request.zk_proof and not validate_zk_proof(request.zk_proof):
            raise SecurityViolationError(f"Invalid proof for task {request.task_id}")
        
        if request.deadline and request.deadline < time.time():
            raise DeadlineExceededError("Task deadline already expired")

    # ----------------- Failure Handling -----------------

    def _handle_task_failure(self, request: TaskRequest, error: Exception):
        """Retry or escalate failed tasks"""
        self.task_retries[request.task_id] += 1
        self.metrics["failed_tasks"].inc()
        
        if self.task_retries[request.task_id] <= self.max_retries:
            logger.warning(f"Retrying task {request.task_id} (attempt "
                          f"{self.task_retries[request.task_id]})")
            asyncio.create_task(self.schedule_task(request))
        else:
            logger.error(f"Task {request.task_id} failed permanently: {str(error)}")
            self._alert_operations(error)

    def _alert_operations(self, error: Exception):
        """Notify monitoring systems about critical failures"""
        # Integrate with PagerDuty/email/Slack
        pass

    # ----------------- Metrics & Monitoring -----------------

    def _update_metrics(self, result: TaskResult):
        """Update Prometheus metrics"""
        self.metrics["current_concurrency"].set(
            self.concurrency_sem._value  # type: ignore
        )
        
        if result.status == "COMPLETED":
            metrics.Histogram("task_duration_seconds").observe(
                result.metrics.get("processing_time", 0)
            )
        elif result.status == "FAILED":
            metrics.Counter("task_failures_total").inc()

    # ----------------- Result Polling -----------------

    async def _poll_result(self, task_id: str) -> TaskResult:
        """Wait for task completion via async queue"""
        while True:
            result = await self.completed_tasks.get()
            if result.task_id == task_id:
                return result
            await self.completed_tasks.put(result)  # Requeue unrelated results

# ----------------- Example Usage -----------------

async def demo_scheduler():
    # Initialize dependencies
    broker = MessageBroker()
    scheduler = TaskScheduler(
        broker,
        encryption_key=Fernet.generate_key(),
        max_retries=3,
        concurrency_limit=100
    )
    
    # Register cluster nodes
    scheduler.register_node(NodeResources(
        node_id="node-1",
        available={"CPU": 8.0, "GPU": 4.0},
        total={"CPU": 8.0, "GPU": 4.0},
        labels={"zone": "us-east1"}
    ))
    
    # Start scheduler loop
    asyncio.create_task(scheduler.run_scheduler_loop())
    
    # Submit sample task
    task = TaskRequest(
        payload={"dataset": "market_data.csv", "model": "LSTM"},
        priority=5,
        resource_requirements={"CPU": 2.0},
        zk_proof="valid_proof_placeholder"
    )
    task_id = await scheduler.schedule_task(task)
    
    # Retrieve result
    result = await scheduler.get_result(task_id)
    print(f"Task completed with status: {result.status}")

if __name__ == "__main__":
    asyncio.run(demo_scheduler())
