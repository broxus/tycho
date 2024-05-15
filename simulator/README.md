## Network simulator

### Prerequisites

- `docker`
- `docker-buildx`
- `docker-compose`

or

- `podman`
- `podman-compose`

### Usage

```bash
cargo install --path ./simulator
# Or alias via `alias simulator="cargo run --bin simulator --"`

simulator prepare
simulator build
simulator node start
simulator node logs -f
simulator node exec
simulator clean
```
