## Network simulator

### Prerequisites

- `docker`
- `docker-buildx`
  or

- `podman`
- `docker-buildx`

- [k3s](https://docs.k3s.io/installation/requirements)
- [helm](https://helm.sh/docs/intro/install/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

> **Warning**  
> DON'T SKIP firewall configuration, otherwise, the simulator will not work
> properly.

### Usage
Prepare:
```bash
# Only for k3s after each reboot (requires sudo)
chmod 644 /etc/rancher/k3s/k3s.yaml
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

Install simulator:
```bash
cargo install --path ./simulator
  # Or alias via `alias simulator="cargo run --bin simulator --"`
```

Use simulator:
```bash
simulator prepare
simulator build
simulator node start
simulator node logs -f
simulator node exec
simulator clean
```

# helm

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
kubectl create ns chaos-mesh
helm install chaos-mesh chaos-mesh/chaos-mesh -n=chaos-mesh --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock --version 2.6.3

helm repo add grafana https://grafana.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update grafana
helm repo update prometheus-community

kubectl create namespace monitoring
helm install grafana grafana/grafana --namespace monitoring --set adminPassword=admin
helm install prometheus prometheus-community/prometheus --namespace monitoring
helm install prometheus-operator prometheus-community/kube-prometheus-stack --namespace monitoring --set alertmanager.enabled=false


export POD_NAME=$(kubectl get pods --namespace monitoring -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=grafana" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace monitoring port-forward $POD_NAME 3000
xdg-open http://localhost:3000

```

Prometheus will scrape all metrics from 9090 port

## How this works in a nutshell

- simulator copies helm chart to `.scratch`, generates `values.yaml` and then
  run helm install