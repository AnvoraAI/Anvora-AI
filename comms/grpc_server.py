# comms/grpc_server.py

import logging
import asyncio
from concurrent import futures
from typing import Any, Dict, Optional, Tuple, Union
import uuid
import time
import grpc
from grpc import aio
from grpc_reflection.v1alpha import reflection

from agents.core.agent_registry import AgentRegistry
from agents.security.zkp_prover import ZKPProver
from protos import agent_pb2, agent_pb2_grpc

logger = logging.getLogger("gRPCServer")

# ----------------- Protocol Buffers Auto-Gen Note -----------------
# Generate with: python -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. protos/agent.proto

# ----------------- Server Configuration -----------------

class ServerConfig:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        max_workers: int = 10,
        max_message_length: int = 1024 * 1024 * 100,  # 100MB
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None
    ):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.options = [
            ("grpc.max_send_message_length", max_message_length),
            ("grpc.max_receive_message_length", max_message_length),
        ]
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key

# ----------------- gRPC Service Implementation -----------------

class AgentService(agent_pb2_grpc.AgentServiceServicer):
    def __init__(
        self, 
        agent_registry: AgentRegistry,
        zkp_prover: ZKPProver
    ):
        self.registry = agent_registry
        self.zkp = zkp_prover
        self.request_counter = 0
        self._lock = asyncio.Lock()
        
    async def _authenticate(
        self, 
        context: grpc.ServicerContext
    ) -> Tuple[bool, Optional[str]]:
        """ZKP-based authentication for each call"""
        metadata = dict(context.invocation_metadata())
        if "zkp-proof" not in metadata:
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "Missing ZKP proof in metadata"
            )
            return False, None
            
        proof = metadata["zkp-proof"]
        if not self.zkp.verify_proof(proof):
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                "Invalid ZKP proof"
            )
            return False, None
            
        return True, metadata.get("client-id")

    async def RegisterAgent(
        self, 
        request: agent_pb2.AgentRegistration,
        context: grpc.ServicerContext
    ) -> agent_pb2.RegistrationResponse:
        """Implements agent registration protocol"""
        auth_valid, client_id = await self._authenticate(context)
        if not auth_valid:
            return agent_pb2.RegistrationResponse(
                success=False,
                error="Authentication failed"
            )

        try:
            async with self._lock:
                agent_id = str(uuid.uuid4())
                await self.registry.register_agent(
                    agent_id=agent_id,
                    agent_type=request.agent_type,
                    capabilities=list(request.capabilities),
                    metadata={
                        "client_id": client_id,
                        "ip": context.peer(),
                        "version": request.version
                    }
                )
                
                return agent_pb2.RegistrationResponse(
                    agent_id=agent_id,
                    success=True,
                    timestamp=int(time.time())
                )
                
        except Exception as e:
            logger.error(f"Registration failed: {str(e)}")
            await context.abort(
                grpc.StatusCode.INTERNAL,
                f"Registration error: {str(e)}"
            )

    async def StreamAgentUpdates(
        self, 
        request_iterator: Any,
        context: grpc.ServicerContext
    ) -> agent_pb2.AgentUpdateSummary:
        """Bidirectional streaming for real-time agent updates"""
        auth_valid, _ = await self._authenticate(context)
        if not auth_valid:
            return
            
        try:
            async for request in request_iterator:
                self.request_counter += 1
                update_result = await self.registry.update_agent_status(
                    agent_id=request.agent_id,
                    status=agent_pb2.AgentStatus.Name(request.status),
                    load_metrics=dict(request.metrics)
                )
                
                yield agent_pb2.AgentUpdateAck(
                    sequence_id=self.request_counter,
                    timestamp=int(time.time()),
                    success=update_result
                )
                
        except grpc.RpcError as e:
            logger.error(f"Stream error: {e.code()}: {e.details()}")
            raise

# ----------------- Health Check Service -----------------

class HealthService(agent_pb2_grpc.HealthServicer):
    async def Check(
        self, 
        request: agent_pb2.HealthCheckRequest,
        context: grpc.ServicerContext
    ) -> agent_pb2.HealthCheckResponse:
        return agent_pb2.HealthCheckResponse(
            status=agent_pb2.HealthCheckResponse.SERVING
        )

# ----------------- Interceptors -----------------

class AuthInterceptor(aio.ServerInterceptor):
    async def intercept_service(
        self,
        continuation: Any,
        handler_call_details: aio.HandlerCallDetails
    ) -> Any:
        method = handler_call_details.method
        logger.info(f"Intercepted call to {method}")
        
        # Skip auth for health checks
        if "Health" in method:
            return await continuation(handler_call_details)
            
        metadata = dict(handler_call_details.invocation_metadata)
        if "api-key" not in metadata:
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "Missing API key"
            )
            
        return await continuation(handler_call_details)

# ----------------- Server Management -----------------

class GRPCServer:
    def __init__(
        self, 
        config: ServerConfig,
        agent_registry: AgentRegistry,
        zkp_prover: ZKPProver
    ):
        self.config = config
        self.server = None
        self._services = [
            AgentService(agent_registry, zkp_prover),
            HealthService()
        ]
        
    async def start(self) -> None:
        server = aio.server(
            futures.ThreadPoolExecutor(max_workers=self.config.max_workers),
            interceptors=(AuthInterceptor(),),
            options=self.config.options
        )
        
        # Add services
        agent_pb2_grpc.add_AgentServiceServicer_to_server(
            self._services[0], server)
        agent_pb2_grpc.add_HealthServicer_to_server(
            self._services[1], server)
            
        # Enable reflection
        SERVICE_NAMES = (
            agent_pb2.DESCRIPTOR.services_by_name['AgentService'].full_name,
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(SERVICE_NAMES, server)
        
        # Configure port
        listen_addr = f"{self.config.host}:{self.config.port}"
        if self.config.ssl_cert and self.config.ssl_key:
            with open(self.config.ssl_key, 'rb') as f:
                private_key = f.read()
            with open(self.config.ssl_cert, 'rb') as f:
                certificate_chain = f.read()
            server_credentials = grpc.ssl_server_credentials(
                [(private_key, certificate_chain)])
            server.add_secure_port(listen_addr, server_credentials)
        else:
            server.add_insecure_port(listen_addr)
            
        await server.start()
        self.server = server
        logger.info(f"gRPC server started on {listen_addr}")
        
    async def stop(self) -> None:
        if self.server:
            await self.server.stop(5)  # 5s grace period
            logger.info("gRPC server stopped")

# ----------------- Example Usage -----------------

async def main():
    # Initialize dependencies
    registry = AgentRegistry()
    zkp = ZKPProver()
    
    # Configure server
    config = ServerConfig(
        port=50051,
        ssl_cert="certs/server.pem",
        ssl_key="certs/server.key"
    )
    server = GRPCServer(config, registry, zkp)
    
    try:
        await server.start()
        await server.server.wait_for_termination()
    except KeyboardInterrupt:
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())
