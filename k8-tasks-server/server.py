"""
MCP server that lets Claude read Kubernetes state (pods, logs, events, workloads,
services, nodes, config, metrics) from prod and nonprod clusters.

Read-only by design: no exec, no delete, no writes, no config changes. Cluster access
is controlled entirely by your existing kubeconfig and AWS/IAM credentials -- this
server does not add any new permissions, it can only do what your role can already do.

Deliberately NOT exposed: Secrets (even names), exec/attach, port-forward, and any
write/patch/delete verb.

Environment names ("prod" / "nonprod") are mapped to actual kubeconfig context names
via env vars so ambiguous context names (e.g. multiple clusters named "prod-*") are
resolved explicitly rather than guessed:

    K8S_PROD_CONTEXT    - kubeconfig context to use for "prod"
    K8S_NONPROD_CONTEXT - kubeconfig context to use for "nonprod"
    KUBECONFIG          - optional path to kubeconfig file (defaults to ~/.kube/config)
"""

import os
import subprocess
import threading
import time
from typing import Callable, Optional, TypeVar

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

mcp = FastMCP("k8s-log-mcp")

MAX_TAIL_LINES = 2000
DEFAULT_TAIL_LINES = 200
MAX_EVENTS = 100
WORKLOAD_KINDS = ("deployment", "replicaset", "statefulset", "daemonset")

# All tools in this server are read-only: no exec, no delete, no writes. These
# annotations tell MCP clients that as a hint (some clients use it to reduce
# confirmation friction for read-only tools) -- it does not itself change what
# the code is allowed to do.
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True)
READ_ONLY_LOCAL = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)

ENVIRONMENTS = {
    "prod": os.environ.get("K8S_PROD_CONTEXT", "prod"),
    "nonprod": os.environ.get("K8S_NONPROD_CONTEXT", "nonprod"),
}

# Set this to the AWS SSO session name (see `sso-session` blocks in ~/.aws/config)
# so the server can attempt an automatic `aws sso login` when it hits an auth error.
AWS_SSO_SESSION = os.environ.get("AWS_SSO_SESSION", "")
SSO_LOGIN_TIMEOUT_SECONDS = 180
SSO_RETRY_COOLDOWN_SECONDS = 60

_sso_lock = threading.Lock()
_last_sso_attempt = 0.0

T = TypeVar("T")


def _resolve_context(environment: str) -> str:
    if environment not in ENVIRONMENTS:
        valid = ", ".join(sorted(ENVIRONMENTS))
        raise ValueError(f"Unknown environment '{environment}'. Valid options: {valid}")
    return ENVIRONMENTS[environment]


def _api_client_for(environment: str) -> client.ApiClient:
    """Build an ApiClient bound to the kubeconfig context for the given environment.

    Uses a fresh Configuration object per call so contexts never bleed into each other
    (kubernetes-client's default load_kube_config mutates global state otherwise).
    Callers wrap this in whichever typed *Api class they need (CoreV1Api, AppsV1Api, ...).
    """
    context_name = _resolve_context(environment)
    configuration = client.Configuration()
    try:
        config.load_kube_config(context=context_name, client_configuration=configuration)
    except ConfigException as e:
        raise RuntimeError(
            f"Could not load kubeconfig context '{context_name}' for environment "
            f"'{environment}': {e}. Run 'kubectl config get-contexts' to see available "
            f"contexts, and 'aws eks update-kubeconfig ...' / 'aws sso login' if the "
            f"context is missing or credentials expired."
        ) from e
    return client.ApiClient(configuration)


def _friendly_api_error(e: ApiException, environment: str) -> str:
    if e.status == 401 or e.status == 403:
        return (
            f"Auth error talking to '{environment}' cluster (HTTP {e.status}). "
            f"Your AWS/EKS credentials may have expired -- try 'aws sso login' "
            f"or refresh the relevant AWS profile, then retry."
        )
    if e.status == 404:
        return f"Not found in '{environment}' cluster: {e.reason}"
    return f"Kubernetes API error in '{environment}' cluster (HTTP {e.status}): {e.reason}"


def _try_refresh_sso() -> tuple[bool, str]:
    """Attempt `aws sso login` once, rate-limited, to recover from expired credentials.

    This only refreshes the local AWS SSO token cache -- it does not touch Kubernetes.
    If the browser session with the IdP is still valid this completes silently in a
    few seconds; otherwise it opens a browser window and waits for you to approve it.
    """
    global _last_sso_attempt
    if not AWS_SSO_SESSION:
        return False, "AWS_SSO_SESSION is not configured, so auto-refresh is disabled."

    with _sso_lock:
        now = time.time()
        if now - _last_sso_attempt < SSO_RETRY_COOLDOWN_SECONDS:
            return False, "An SSO refresh was already attempted recently; please wait a bit and retry."
        _last_sso_attempt = now
        try:
            result = subprocess.run(
                ["aws", "sso", "login", "--sso-session", AWS_SSO_SESSION],
                capture_output=True,
                text=True,
                timeout=SSO_LOGIN_TIMEOUT_SECONDS,
            )
        except FileNotFoundError:
            return False, "aws CLI not found on PATH."
        except subprocess.TimeoutExpired:
            return False, "aws sso login timed out waiting for browser approval."

    if result.returncode == 0:
        return True, "AWS SSO session refreshed successfully."
    return False, f"aws sso login failed: {(result.stderr or result.stdout).strip()}"


def _call_k8s(environment: str, action: Callable[[client.ApiClient], T]) -> T:
    """Run a Kubernetes API call, auto-refreshing AWS SSO once and retrying on 401/403."""
    try:
        return action(_api_client_for(environment))
    except ApiException as e:
        if e.status not in (401, 403):
            raise RuntimeError(_friendly_api_error(e, environment)) from e
        refreshed, message = _try_refresh_sso()
        if not refreshed:
            raise RuntimeError(f"{_friendly_api_error(e, environment)} (auto-refresh: {message})") from e
        try:
            return action(_api_client_for(environment))
        except ApiException as e2:
            raise RuntimeError(_friendly_api_error(e2, environment)) from e2


def _event_summary(event) -> dict:
    return {
        "type": event.type,
        "reason": event.reason,
        "message": event.message,
        "count": event.count,
        "involved_object": f"{event.involved_object.kind}/{event.involved_object.name}",
        "last_timestamp": str(event.last_timestamp or event.event_time),
    }


def _container_status_summary(status) -> dict:
    state = status.state
    if state.running:
        state_summary = {"state": "running", "started_at": str(state.running.started_at)}
    elif state.waiting:
        state_summary = {"state": "waiting", "reason": state.waiting.reason, "message": state.waiting.message}
    elif state.terminated:
        state_summary = {
            "state": "terminated",
            "reason": state.terminated.reason,
            "exit_code": state.terminated.exit_code,
            "message": state.terminated.message,
        }
    else:
        state_summary = {"state": "unknown"}
    return {
        "name": status.name,
        "ready": status.ready,
        "restart_count": status.restart_count,
        "image": status.image,
        **state_summary,
    }


@mcp.tool(annotations=READ_ONLY_LOCAL)
def list_environments() -> dict:
    """List the configured environment names (e.g. prod, nonprod) and the kubeconfig
    context each one maps to."""
    return ENVIRONMENTS


@mcp.tool(annotations=READ_ONLY_LOCAL)
def list_kube_contexts() -> list:
    """List every context available in the local kubeconfig, and which one is
    currently active. Useful for figuring out the right context name to map an
    environment to."""
    contexts, active = config.list_kube_config_contexts()
    active_name = active["name"] if active else None
    return [{"name": c["name"], "active": c["name"] == active_name} for c in contexts]


@mcp.tool(annotations=READ_ONLY)
def list_namespaces(environment: str) -> list:
    """List all namespace names in the given environment ('prod' or 'nonprod')."""
    result = _call_k8s(environment, lambda api: client.CoreV1Api(api).list_namespace())
    return [ns.metadata.name for ns in result.items]


@mcp.tool(annotations=READ_ONLY)
def list_pods(environment: str, namespace: str, label_selector: str = "") -> list:
    """List pods in a namespace for the given environment ('prod' or 'nonprod'),
    including status, ready state, restart count, and container names.

    label_selector: optional Kubernetes label selector, e.g. "app=my-service".
    """
    result = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).list_namespaced_pod(namespace=namespace, label_selector=label_selector or None),
    )

    pods = []
    for pod in result.items:
        statuses = pod.status.container_statuses or []
        pods.append({
            "name": pod.metadata.name,
            "phase": pod.status.phase,
            "containers": [c.name for c in pod.spec.containers],
            "ready": sum(1 for s in statuses if s.ready),
            "total_containers": len(statuses),
            "restarts": sum(s.restart_count for s in statuses),
            "node": pod.spec.node_name,
        })
    return pods


@mcp.tool(annotations=READ_ONLY)
def get_pod_logs(
    environment: str,
    namespace: str,
    pod_name: str,
    container: str = "",
    tail_lines: int = DEFAULT_TAIL_LINES,
    previous: bool = False,
    since_seconds: Optional[int] = None,
) -> str:
    """Fetch logs for a pod in the given environment ('prod' or 'nonprod').

    container: container name (required if the pod has more than one container).
    tail_lines: number of most recent lines to return (capped at 2000).
    previous: if True, get logs from the previously terminated container instance
        (useful for diagnosing crash loops).
    since_seconds: if set, only return logs newer than this many seconds ago.
    """
    tail_lines = max(1, min(tail_lines, MAX_TAIL_LINES))
    return _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container or None,
            tail_lines=tail_lines,
            previous=previous,
            since_seconds=since_seconds,
            timestamps=True,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def get_events(environment: str, namespace: str, object_name: str = "", kind: str = "") -> list:
    """List recent Kubernetes events in a namespace -- scheduling failures, image pull
    errors, OOMKills, probe failures, etc. Often the fastest way to find out *why*
    something is broken, more useful than logs for scheduling/lifecycle issues.

    object_name: optional, filter to events about one object (e.g. a pod or deployment name).
    kind: optional, filter to events about objects of this kind (e.g. "Pod", "Deployment").
    """
    selectors = []
    if object_name:
        selectors.append(f"involvedObject.name={object_name}")
    if kind:
        selectors.append(f"involvedObject.kind={kind}")
    field_selector = ",".join(selectors) or None

    result = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).list_namespaced_event(
            namespace=namespace, field_selector=field_selector, limit=MAX_EVENTS
        ),
    )
    events = [_event_summary(e) for e in result.items]
    events.sort(key=lambda e: e["last_timestamp"], reverse=True)
    return events


@mcp.tool(annotations=READ_ONLY)
def describe_pod(environment: str, namespace: str, pod_name: str) -> dict:
    """Get full detail for one pod: spec (images, resource requests/limits, node,
    volumes), status (phase, conditions, per-container state/restarts), and recent
    related events. Equivalent to `kubectl describe pod`.

    Note: container env var *names* are included but values are omitted, since pod
    specs can contain literal (non-Secret) sensitive values.
    """
    pod = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).read_namespaced_pod(name=pod_name, namespace=namespace),
    )
    events = get_events(environment, namespace, object_name=pod_name, kind="Pod")

    containers = [{
        "name": c.name,
        "image": c.image,
        "resources": {
            "requests": (c.resources.requests or {}) if c.resources else {},
            "limits": (c.resources.limits or {}) if c.resources else {},
        },
        "env_names": [e.name for e in (c.env or [])],
    } for c in pod.spec.containers]

    volume_names = [v.name for v in (pod.spec.volumes or [])]

    conditions = [{
        "type": c.type, "status": c.status, "reason": c.reason, "message": c.message,
    } for c in (pod.status.conditions or [])]

    return {
        "name": pod.metadata.name,
        "namespace": pod.metadata.namespace,
        "node": pod.spec.node_name,
        "service_account": pod.spec.service_account_name,
        "phase": pod.status.phase,
        "pod_ip": pod.status.pod_ip,
        "host_ip": pod.status.host_ip,
        "start_time": str(pod.status.start_time),
        "qos_class": pod.status.qos_class,
        "containers": containers,
        "volume_names": volume_names,
        "conditions": conditions,
        "container_statuses": [_container_status_summary(s) for s in (pod.status.container_statuses or [])],
        "recent_events": events[:20],
    }


_WORKLOAD_LISTERS = {
    "deployment": lambda apps, ns: apps.list_namespaced_deployment(namespace=ns),
    "replicaset": lambda apps, ns: apps.list_namespaced_replica_set(namespace=ns),
    "statefulset": lambda apps, ns: apps.list_namespaced_stateful_set(namespace=ns),
    "daemonset": lambda apps, ns: apps.list_namespaced_daemon_set(namespace=ns),
}


@mcp.tool(annotations=READ_ONLY)
def list_workloads(environment: str, namespace: str, kind: str = "deployment") -> list:
    """List workload controllers (Deployments, ReplicaSets, StatefulSets, or DaemonSets)
    in a namespace, with desired/ready/available replica counts and container images --
    useful for spotting stuck or partial rollouts.

    kind: one of "deployment", "replicaset", "statefulset", "daemonset".
    """
    if kind not in _WORKLOAD_LISTERS:
        raise ValueError(f"kind must be one of {WORKLOAD_KINDS}")

    result = _call_k8s(environment, lambda api: _WORKLOAD_LISTERS[kind](client.AppsV1Api(api), namespace))

    workloads = []
    for w in result.items:
        status = w.status
        images = [c.image for c in w.spec.template.spec.containers]
        workloads.append({
            "name": w.metadata.name,
            "desired": getattr(w.spec, "replicas", None) or getattr(status, "desired_number_scheduled", None),
            "ready": getattr(status, "ready_replicas", None) or getattr(status, "number_ready", None) or 0,
            "available": getattr(status, "available_replicas", None) or getattr(status, "number_available", None) or 0,
            "updated": getattr(status, "updated_replicas", None) or getattr(status, "updated_number_scheduled", None) or 0,
            "images": images,
        })
    return workloads


@mcp.tool(annotations=READ_ONLY)
def list_services(environment: str, namespace: str) -> list:
    """List Services in a namespace: type, cluster IP, ports, and pod selector."""
    result = _call_k8s(environment, lambda api: client.CoreV1Api(api).list_namespaced_service(namespace=namespace))
    return [{
        "name": s.metadata.name,
        "type": s.spec.type,
        "cluster_ip": s.spec.cluster_ip,
        "ports": [{"port": p.port, "target_port": str(p.target_port), "protocol": p.protocol} for p in (s.spec.ports or [])],
        "selector": s.spec.selector,
    } for s in result.items]


@mcp.tool(annotations=READ_ONLY)
def get_service_endpoints(environment: str, namespace: str, service_name: str) -> dict:
    """Show which pod IPs are actually backing a Service (ready vs not-ready). An empty
    'ready_addresses' list on a Service that should have traffic is a common root cause
    for "service unreachable" issues (bad selector, failing readiness probes, etc.)."""
    ep = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).read_namespaced_endpoints(name=service_name, namespace=namespace),
    )
    ready, not_ready = [], []
    for subset in (ep.subsets or []):
        ready.extend(a.ip for a in (subset.addresses or []))
        not_ready.extend(a.ip for a in (subset.not_ready_addresses or []))
    return {"service": service_name, "ready_addresses": ready, "not_ready_addresses": not_ready}


@mcp.tool(annotations=READ_ONLY)
def list_nodes(environment: str) -> list:
    """List cluster nodes with readiness, capacity/allocatable resources, and taints --
    useful when pods are stuck Pending or getting evicted."""
    result = _call_k8s(environment, lambda api: client.CoreV1Api(api).list_node())
    nodes = []
    for n in result.items:
        conditions = {c.type: c.status for c in (n.status.conditions or [])}
        nodes.append({
            "name": n.metadata.name,
            "conditions": conditions,
            "capacity": n.status.capacity,
            "allocatable": n.status.allocatable,
            "taints": [f"{t.key}={t.value}:{t.effect}" for t in (n.spec.taints or [])],
            "kubelet_version": n.status.node_info.kubelet_version if n.status.node_info else None,
        })
    return nodes


@mcp.tool(annotations=READ_ONLY)
def describe_node(environment: str, node_name: str) -> dict:
    """Full detail for one node: conditions, capacity/allocatable, taints, and recent
    node-related events (e.g. disk pressure, kubelet issues)."""
    node = _call_k8s(environment, lambda api: client.CoreV1Api(api).read_node(name=node_name))
    events_result = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).list_event_for_all_namespaces(
            field_selector=f"involvedObject.name={node_name},involvedObject.kind=Node", limit=MAX_EVENTS
        ),
    )
    events = sorted((_event_summary(e) for e in events_result.items), key=lambda e: e["last_timestamp"], reverse=True)
    conditions = [{
        "type": c.type, "status": c.status, "reason": c.reason, "message": c.message,
    } for c in (node.status.conditions or [])]
    return {
        "name": node.metadata.name,
        "conditions": conditions,
        "capacity": node.status.capacity,
        "allocatable": node.status.allocatable,
        "taints": [f"{t.key}={t.value}:{t.effect}" for t in (node.spec.taints or [])],
        "kubelet_version": node.status.node_info.kubelet_version if node.status.node_info else None,
        "recent_events": events[:20],
    }


@mcp.tool(annotations=READ_ONLY)
def list_configmaps(environment: str, namespace: str) -> list:
    """List ConfigMaps in a namespace with their key names only (no values). Use
    get_configmap to read the actual values of a specific ConfigMap."""
    result = _call_k8s(environment, lambda api: client.CoreV1Api(api).list_namespaced_config_map(namespace=namespace))
    return [{"name": cm.metadata.name, "keys": sorted((cm.data or {}).keys())} for cm in result.items]


@mcp.tool(annotations=READ_ONLY)
def get_configmap(environment: str, namespace: str, name: str) -> dict:
    """Read the full contents (including values) of one ConfigMap. Read-only -- this
    cannot modify the ConfigMap. Note: ConfigMaps aren't intended for secrets, but
    double-check before sharing output if your team stores sensitive config here."""
    cm = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).read_namespaced_config_map(name=name, namespace=namespace),
    )
    return {"name": cm.metadata.name, "data": cm.data or {}, "binary_data_keys": sorted((cm.binary_data or {}).keys())}


@mcp.tool(annotations=READ_ONLY)
def get_pod_metrics(environment: str, namespace: str) -> list:
    """Show live CPU/memory usage per pod (like `kubectl top pods`). Requires
    metrics-server to be installed in the cluster -- returns a clear error if not."""
    try:
        result = _call_k8s(
            environment,
            lambda api: client.CustomObjectsApi(api).list_namespaced_custom_object(
                group="metrics.k8s.io", version="v1beta1", namespace=namespace, plural="pods"
            ),
        )
    except RuntimeError as e:
        if "404" in str(e):
            raise RuntimeError(
                f"metrics.k8s.io API not found in '{environment}' -- is metrics-server installed?"
            ) from e
        raise
    return [{
        "name": item["metadata"]["name"],
        "containers": [
            {"name": c["name"], "cpu": c["usage"]["cpu"], "memory": c["usage"]["memory"]}
            for c in item["containers"]
        ],
    } for item in result.get("items", [])]


@mcp.tool(annotations=READ_ONLY)
def list_persistent_volume_claims(environment: str, namespace: str) -> list:
    """List PersistentVolumeClaims in a namespace: bound status, capacity, storage class."""
    result = _call_k8s(
        environment,
        lambda api: client.CoreV1Api(api).list_namespaced_persistent_volume_claim(namespace=namespace),
    )
    return [{
        "name": pvc.metadata.name,
        "phase": pvc.status.phase,
        "capacity": (pvc.status.capacity or {}).get("storage"),
        "storage_class": pvc.spec.storage_class_name,
        "access_modes": pvc.spec.access_modes,
        "volume_name": pvc.spec.volume_name,
    } for pvc in result.items]


@mcp.tool(annotations=READ_ONLY)
def get_hpa_status(environment: str, namespace: str) -> list:
    """List HorizontalPodAutoscalers in a namespace: target workload, min/max/current
    replicas, and current vs target metrics -- useful for diagnosing scaling issues."""
    result = _call_k8s(
        environment,
        lambda api: client.AutoscalingV2Api(api).list_namespaced_horizontal_pod_autoscaler(namespace=namespace),
    )
    hpas = []
    for hpa in result.items:
        metrics = []
        for m in (hpa.status.current_metrics or []):
            if m.resource:
                metrics.append({"resource": m.resource.name, "current": m.resource.current.average_utilization})
        hpas.append({
            "name": hpa.metadata.name,
            "target": f"{hpa.spec.scale_target_ref.kind}/{hpa.spec.scale_target_ref.name}",
            "min_replicas": hpa.spec.min_replicas,
            "max_replicas": hpa.spec.max_replicas,
            "current_replicas": hpa.status.current_replicas,
            "desired_replicas": hpa.status.desired_replicas,
            "current_metrics": metrics,
        })
    return hpas


if __name__ == "__main__":
    mcp.run(transport="stdio")
