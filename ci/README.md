# Tycho Testing Environment

This directory contains Docker Compose setup for running a 3-node Tycho network
and test containers, as well as CI scripts used in GitHub Actions workflows.

## Available Test Containers

- `tycho-tests-ping-pong:sha-1700a07`: Runs ping-pong tests against the network
- `tycho-tests-destroyable:sha-1700a07`: Runs destroyable contract tests against
  the network

## CI Scripts

- `run_network_tests.sh` - Script for running network tests using Docker. It:
  1. Starts Tycho nodes using `scripts/run_nodes.sh`
  2. Waits for the network to stabilize
  3. Runs network tests from the `ghcr.io/broxus/tycho-tests/tycho-tests-destroyable:latest` Docker image

## Usage

### 1. **Create a PAT with `read:packages` Scope via GitHub CLI**

Use `gh api` to create a token programmatically:

1. Go to
   GitHub [Settings → Developer settings → Tokens (classic)](https://github.com/settings/tokens/new)
2. Under **Select scopes**, enable:
    - `read:packages` (to access container registry)
    - `write:packages` (if you need to push packages)
3. Generate token and **copy it immediately** - you won't see it again

---

### 2. **Log in to GHCR with Docker/Podman**

Use the generated token to authenticate:

#### For **Docker**:

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
```

#### For **Podman**:

```bash
echo "$GHCR_TOKEN" | podman login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
```

Replace `YOUR_GITHUB_USERNAME` with your GitHub username.