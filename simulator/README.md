## Network simulator

### Prerequisites
Simulator depends on project structure and works only inside git project root:
```bash
git clone https://github.com/broxus/tycho.git
cd ./tycho
```

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
# Generate zerostate config stub (with optional --force flag):
just init_zerostate_config
# Generate node config sub (with optional --force flag):
just init_node_config
# Generate a network of 4 nodes (with optional --force flag):
just gen_network 4
# Only for k3s after each reboot (requires sudo)
chmod 644 /etc/rancher/k3s/k3s.yaml
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

Install simulator:
```bash
cargo install --path ./simulator
  # Or alias via `alias simulator="cargo run --bin simulator --"`
```

At this point you may edit default values in generated files before they are applied to helm templates:
* [${git_project_root}/config.json](../config.json)
* [${git_project_root}/logger.json](../logger.json)

Use simulator:
```bash
simulator build
simulator prepare
simulator node start
simulator node logs -f
simulator node exec -n 0 -it /bin/bash
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
````

Prometheus will scrape all metrics from 9090 port

Open Prometheus UI to check `tycho` target scraping health:
```bash
export POD_NAME=$(kubectl get pods --namespace monitoring -l "app.kubernetes.io/name=prometheus,app.kubernetes.io/instance=prometheus" -o jsonpath="{.items[0].metadata.name}")
xdg-open http://localhost:9090/targets?search=tycho
kubectl --namespace monitoring port-forward $POD_NAME 9090
```

Open Grafana to add Prometheus datasource:
```bash
export POD_NAME=$(kubectl get pods --namespace monitoring -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=grafana" -o jsonpath="{.items[0].metadata.name}")
xdg-open http://localhost:3000/connections/datasources/new
kubectl --namespace monitoring port-forward $POD_NAME 3000
```

Set Prometheus Url to `http://${CLUSTER-IP}:9090`, latest version, no auth. Push `Save and test` button.
```
$ kubectl get service prometheus-operator-kube-p-prometheus -n monitoring
NAME                                    TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
prometheus-operator-kube-p-prometheus   ClusterIP   10.43.80.191   <none>        9090/TCP,8080/TCP   8d
```
###### If name changed, find a new one with `kubectl get svc -n monitoring`: the single entry with `9090/TCP` and defined `CLUSTER-IP`.

Generate and import dashboard:
```bash
cd ../scripts/
./install-python-deps.sh
python ./gen-dashboard.py | wl-copy
xdg-open http://localhost:3000/dashboard/import
```

## How this works in a nutshell

- simulator copies helm chart to `.scratch`, generates `values.yaml` and then
  run helm install