### Why

Your image build locally may not run in k8s because of `Docker exit code 132 (Illegal instruction)`.
Then you have to build both the base and tycho images in k8s and push it into your image registry.

This chart is based on [Kubernetes manifests for BuildKit](https://github.com/moby/buildkit/tree/master/examples/kubernetes).

### Prerequisites: image registry auth

Create a secret named `tycho-docker-config` with `.dockerconfigjson` field of special k8s type.

From local docker config:
```shell
kubectl create secret docker-registry tycho-docker-config --from-file="~/.docker/config.json"
```

Or from command line:
```shell
kubectl create secret docker-registry tycho-docker-config \
  --docker-server="https://index.docker.io/v1/" \
  --docker-username="username" \
  --docker-password="password or auth token"
```

Notes:
* buildkit doesn't use old-fashioned `.dockercfg` file format
* buildkit expects the registry server in URL format and the `auth` field; other fields may be omitted
* `https://index.docker.io/v1/` is a proper URL for DockerHub
* you may use an access token instead of password, especially if you use 2FA
* you may change secret name in `./values.yaml`; it is used both to pull and push images

See also: 
* [Kubernetes: Log in to Docker Hub](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/#log-in-to-docker-hub)
* [Helm Tips and Tricks: Creating Image Pull Secrets](https://helm.sh/docs/howto/charts_tips_and_tricks/#creating-image-pull-secrets)