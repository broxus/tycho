## Usage

### 1. **Create a PAT with `read:packages`**

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

### How to test:

```bash
 scripts/run_network_tests.sh ghcr.io/broxus/tycho-tests/tycho-tests-ping-pong:latest
 ```

set `CI=true` to run tests using podman.

```bash
CI=true scripts/run_network_tests.sh ghcr.io/broxus/tycho-tests/tycho-tests-destroyable:latest
```