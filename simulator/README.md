## Network simulator

### How this works in a nutshell
Simulator generates default `values.yaml` and wraps helm and kubectl commands for shorthand.

### Prerequisites
Simulator depends on project structure and works only inside git project root:
```shell
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

Helm monitoring should be installed before simulator is run.

### Usage
Prepare:
```shell
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
```shell
cargo install --path ./simulator
  # Or alias via `alias simulator="cargo run --bin simulator --"`
```

At this point you may edit default values in generated files before they are applied to helm templates:
* [${git_project_root}/config.json](../config.json)
* [${git_project_root}/logger.json](../logger.json)

Use simulator:
```shell
simulator prepare
simulator build local
simulator start
simulator node logs -f
simulator node shell -n 0
simulator stop
simulator clean
```

# helm monitoring

```shell
helm repo add vm https://victoriametrics.github.io/helm-charts/
helm repo update vm
kubectl create namespace monitoring
helm install vmks vm/victoria-metrics-k8s-stack -n monitoring --set vmagent.spec.scrapeInterval=10s --atomic --wait
````

Get your Grafana password for `admin` user:
```shell
kubectl get secret vmks-grafana --template='{{ index .data "admin-password" | base64decode}}' -n monitoring
```

For more details see https://docs.victoriametrics.com/helm/victoriametrics-k8s-stack/#install-operator-separately,
but note that `vmks` release name and `monitoring` namespace are used in `VMPodScrape` to scrape metrics from pods.

Open [VictoriaMetrics UI](http://localhost:8429/targets?endpoint_search=&label_search=%7Bcontainer%3D%22tycho%22%7D)
to check `tycho` target scraping health:
```shell
export POD_NAME=$(kubectl get pods -n monitoring -l "app.kubernetes.io/name=vmagent,app.kubernetes.io/instance=vmks-victoria-metrics-k8s-stack" -o jsonpath="{.items[0].metadata.name}")
kubectl -n monitoring port-forward $POD_NAME 8429
```

Open [Grafana](http://localhost:3000/dashboard/import) to import Tycho dashboard:
```shell
export POD_NAME=$(kubectl get pods -n monitoring -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=vmks" -o jsonpath="{.items[0].metadata.name}")
kubectl -n monitoring port-forward $POD_NAME 3000
```

Generate dashboard into Wayland clipboard:
```shell
../scripts/install-python-deps.sh
python ../scripts/gen-dashboard.py | wl-copy
```

### GKE

- Update cluster to 1.32+ to avoid [gke bug](https://cloud.google.com/kubernetes-engine/docs/troubleshooting/known-issues#pods-using-io-uring-related-syscalls-stuck-in-terminating)
- Enable k8s egress to pull images from public repos, see [StackOverflow answer](https://stackoverflow.com/a/57664750).
- To build the image in k8s, see [builder subchart](./helm/builder/README.md)

Set `kubectl` to the right cluster.

Use `prepare` to create default `values.yaml` that slightly differ from k3s.

Modify generated [builder values](./helm/builder/values.yaml) for your registry and versions.

Every call to `prepare` overwrites [tycho values](./helm/tycho/values.yaml), but lets builder values intact.

Use `build install` to spawn k8s builder job and `remove` to clean up when it finishes.

```shell
export KUBECONFIG=~/.kube/config
kubectl config use-context ${CONTEXT}

simulator prepare
sumulator build install
sumulator build remove

simulator start
simulator stop
```

To disable logs completely just leave empty json object `{}` in logger config.

### Helm chaos-mesh

As per [docs](https://chaos-mesh.org/docs/production-installation-using-helm/):
* chaos dashboard here does not require auth
* `default` namespace is a target for chaos

```shell
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update chaos-mesh
kubectl create ns chaos-mesh

kubectl annotate ns default chaos-mesh.org/inject=enabled
```

For k3s
```shell
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh \
--set chaosDaemon.runtime=containerd \
--set chaosDaemon.socketPath=/run/containerd/containerd.sock \
--set controllerManager.enableFilterNamespace=true \
--set dashboard.securityMode=false \
--atomic --wait
```

For GKE
```shell
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh \
--set controllerManager.enableFilterNamespace=true \
--set dashboard.securityMode=false \
--atomic --wait
```

Open [control dashboard](http://localhost:2333) and create 
[experiments](https://chaos-mesh.org/docs/simulate-pod-chaos-on-kubernetes/):
```shell
kubectl port-forward -n chaos-mesh svc/chaos-dashboard 2333:2333
```