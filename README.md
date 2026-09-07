# k8s-log-mcp

An [MCP](https://modelcontextprotocol.io) server that lets Claude (via Claude Desktop
connectors) inspect Kubernetes clusters — pods, logs, events, workloads, services,
nodes, config, and metrics — across a "prod" and "nonprod" environment.

**Read-only by design.** No exec, no delete, no writes, no config changes. Every tool
only performs Kubernetes `get`/`list` calls. It can only do what your existing
kubeconfig / IAM role already lets you do — this project grants no new permissions.

Deliberately **not** exposed: Secrets (even names), exec/attach, port-forward, and any
write/patch/delete verb.

## Tools

| Tool | Description |
|---|---|
| `list_environments` | Show configured environment → kubeconfig context mapping |
| `list_kube_contexts` | List all contexts in your local kubeconfig |
| `list_namespaces` | List namespaces in an environment |
| `list_pods` | List pods in a namespace (status, restarts, containers) |
| `get_pod_logs` | Fetch container logs, including `previous` (crash-loop) logs |
| `get_events` | Recent Kubernetes events for a namespace or one object |
| `describe_pod` | Full pod detail: spec, status, resources, volumes, recent events |
| `list_workloads` | Deployments/ReplicaSets/StatefulSets/DaemonSets and replica counts |
| `list_services` | Services: type, cluster IP, ports, selector |
| `get_service_endpoints` | Ready vs not-ready pod IPs backing a Service |
| `list_nodes` / `describe_node` | Node conditions, capacity, taints, node events |
| `list_configmaps` / `get_configmap` | ConfigMap key names, or full key/value data |
| `get_pod_metrics` | Live CPU/memory per pod (`kubectl top pods`, needs metrics-server) |
| `list_persistent_volume_claims` | PVC bound status, capacity, storage class |
| `get_hpa_status` | HorizontalPodAutoscaler min/max/current replicas and metrics |

## Requirements

- Python 3.10+
- A working kubeconfig (`~/.kube/config` or `$KUBECONFIG`) with contexts for the
  clusters you want to query
- `kubectl`/cloud CLI already able to authenticate to those clusters (e.g. AWS CLI +
  `aws eks update-kubeconfig` for EKS)

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Set these environment variables when launching the server:

| Variable | Required | Purpose |
|---|---|---|
| `K8S_PROD_CONTEXT` | yes | kubeconfig context name to use for `environment="prod"` |
| `K8S_NONPROD_CONTEXT` | yes | kubeconfig context name to use for `environment="nonprod"` |
| `KUBECONFIG` | no | Path to kubeconfig file (defaults to `~/.kube/config`) |
| `AWS_SSO_SESSION` | no | AWS SSO session name (from `~/.aws/config`); if set, the server will attempt `aws sso login --sso-session <name>` automatically on an auth error and retry once |

Run `kubectl config get-contexts` to see your available context names.

## Using it with Claude Desktop

Add an entry to Claude Desktop's config file
(`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "k8s-log-mcp": {
      "command": "/absolute/path/to/k8s-log-mcp/.venv/bin/python",
      "args": ["/absolute/path/to/k8s-log-mcp/server.py"],
      "env": {
        "K8S_PROD_CONTEXT": "your-prod-context-name",
        "K8S_NONPROD_CONTEXT": "your-nonprod-context-name",
        "AWS_SSO_SESSION": "your-sso-session-name"
      }
    }
  }
}
```

Fully quit and reopen Claude Desktop afterwards (the server runs as a subprocess
launched at app startup, so it won't pick up config or code changes without a
restart). It will then appear as a connector in Claude Desktop.

## Testing without Claude

Since every tool is a plain Python function under the `@mcp.tool()` decorator, you can
sanity-check the Kubernetes integration directly, without going through the MCP
protocol at all:

```bash
K8S_PROD_CONTEXT=... K8S_NONPROD_CONTEXT=... python -c "
import server
print(server.list_namespaces('nonprod'))
"
```

## Security notes

- All tools are annotated `readOnlyHint=True, destructiveHint=False` per the MCP
  tool-annotation spec — advisory to the client, not an enforcement mechanism. The
  actual guarantee is that the code never calls any Kubernetes `create`/`patch`/`delete`
  API.
- `describe_pod` shows container environment variable **names** but not values, since
  pod specs can contain literal (non-Secret) sensitive values.
- `get_configmap` returns full key/value data. ConfigMaps aren't meant for secrets, but
  double-check contents before sharing output if your team stores sensitive config there.
- `get_pod_logs` and `get_events` cap how much data is returned per call to avoid
  flooding the model's context window.
