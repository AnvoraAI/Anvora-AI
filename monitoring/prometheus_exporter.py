# monitoring/prometheus_exporter.py

import asyncio
import logging
from collections import defaultdict
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
)
from aiohttp import web
from prometheus_client import (
    REGISTRY, CollectorRegistry, Counter, Gauge, Histogram, Summary,
    generate_latest, CONTENT_TYPE_LATEST
)
from prometheus_client.metrics import MetricWrapperBase
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend
import uvloop

logger = logging.getLogger("PrometheusExporter")
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class SecureMetricsRegistry:
    """Encrypted metric storage with access controls"""
    def __init__(self):
        self._metrics: Dict[str, MetricWrapperBase] = {}
        self._access_keys: Dict[str, bytes] = {}
        self._lock = asyncio.Lock()

    async def register_metric(self, metric: MetricWrapperBase, 
                            access_token: Optional[str] = None):
        """Register metric with optional access token"""
        async with self._lock:
            if metric._name in self._metrics:
                raise ValueError(f"Metric {metric._name} already exists")
            self._metrics[metric._name] = metric
            if access_token:
                self._access_keys[metric._name] = self._derive_key(access_token)

    def _derive_key(self, token: str) -> bytes:
        """HKDF-based key derivation from access token"""
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b'prometheus-export',
            backend=default_backend()
        ).derive(token.encode())

    async def get_metric(self, name: str, 
                        token: Optional[str] = None) -> MetricWrapperBase:
        """Retrieve metric with optional access verification"""
        metric = self._metrics.get(name)
        if not metric:
            raise KeyError(f"Metric {name} not found")
        
        if name in self._access_keys:
            if not token or not self._verify_token(name, token):
                raise PermissionError("Invalid access token")
        
        return metric

    def _verify_token(self, name: str, token: str) -> bool:
        """Verify token matches stored key"""
        try:
            current_key = self._derive_key(token)
            return self._access_keys[name] == current_key
        except Exception:
            return False

class MetricsExporter:
    """High-performance metrics exporter with security features"""
    def __init__(
        self,
        registry: CollectorRegistry = REGISTRY,
        port: int = 9090,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        auth_token: Optional[str] = None
    ):
        self.registry = registry
        self.port = port
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.auth_token = auth_token
        self.app = web.Application()
        self.app.router.add_get("/metrics", self.handle_metrics)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

        # Pre-register core metrics
        self.request_count = Counter(
            "http_requests_total", 
            "Total HTTP requests",
            ["method", "endpoint", "status"]
        )
        self.request_latency = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency",
            ["method", "endpoint"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        self.registry.register(self)
    
    async def handle_metrics(self, request: web.Request) -> web.Response:
        """Handle metrics endpoint with security checks"""
        if self.auth_token:
            token = request.headers.get("Authorization", "").replace("Bearer ", "")
            if token != self.auth_token:
                return web.Response(status=401, text="Unauthorized")

        with self.request_latency.labels(request.method, "/metrics").time():
            self.request_count.labels(request.method, "/metrics", "200").inc()
            try:
                data = generate_latest(self.registry)
                return web.Response(
                    body=data,
                    content_type=CONTENT_TYPE_LATEST
                )
            except Exception as e:
                self.request_count.labels(request.method, "/metrics", "500").inc()
                logger.error(f"Metrics generation failed: {str(e)}")
                return web.Response(status=500, text="Internal Server Error")

    async def start(self):
        """Start metrics server with TLS if configured"""
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        ssl_context = None
        if self.tls_cert and self.tls_key:
            ssl_context = web_ssl_context(
                certfile=self.tls_cert,
                keyfile=self.tls_key
            )
        self._site = web.TCPSite(
            self._runner, 
            port=self.port, 
            ssl_context=ssl_context
        )
        await self._site.start()
        logger.info(f"Metrics exporter running on port {self.port}")

    async def stop(self):
        """Graceful shutdown"""
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        logger.info("Metrics exporter stopped")

def web_ssl_context(
    certfile: str, 
    keyfile: str
) -> Optional[web.SecurityConfig]:
    """Create SSL context for aiohttp"""
    context = web.BaseSSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile, keyfile)
    context.set_ciphers('ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384')
    context.set_alpn_protocols(['h2', 'http/1.1'])
    return context

class CustomCollector:
    """Collect application-specific metrics from Gradual AGENT components"""
    def __init__(self, registry: SecureMetricsRegistry):
        self.registry = registry
        self._metrics = [
            ("gradual_agent_tasks_total", Counter, "Total tasks processed"),
            ("gradual_agent_task_duration_seconds", Histogram, 
             "Task processing time distribution", ["task_type"], [0.1, 0.5, 1, 5]),
            ("gradual_agent_nodes_online", Gauge, "Current online nodes"),
            ("gradual_agent_security_violations_total", Counter,
             "Total security policy violations"),
        ]

    def collect(self):
        """Generate metrics for Prometheus collection"""
        try:
            for metric_def in self._metrics:
                name = metric_def[0]
                metric_type = metric_def[1]
                description = metric_def[2]
                labels = metric_def[3] if len(metric_def) > 3 else []
                buckets = metric_def[4] if len(metric_def) > 4 else None

                metric = metric_type(
                    name, 
                    description, 
                    labels=labels,
                    buckets=buckets
                )
                # Populate real-time data from application state
                if name == "gradual_agent_nodes_online":
                    metric.set(self._get_node_count())
                elif name == "gradual_agent_security_violations_total":
                    metric.inc(self._get_security_events())
                
                yield metric
        except Exception as e:
            logger.error(f"Metrics collection error: {str(e)}")

    def _get_node_count(self) -> int:
        """Get current cluster node count (mock implementation)"""
        return 42  # Replace with real cluster state query

    def _get_security_events(self) -> int:
        """Get security violation count (mock implementation)"""
        return 7  # Replace with real security monitor query

async def start_exporter():
    """Example startup sequence"""
    exporter = MetricsExporter(
        port=9090,
        tls_cert="path/to/cert.pem",
        tls_key="path/to/key.pem",
        auth_token="secure-token-123"
    )
    await exporter.start()
    return exporter

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        exporter = loop.run_until_complete(start_exporter())
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(exporter.stop())
    finally:
        loop.close()
