# Become a Validator

## Recommended Hardware Requirements

- 16 cores \ 32 threads CPU
- 128 GB RAM
- 1TB Fast NVMe SSD
- 1 Gbit/s network connectivity
- Public IP address

## Prerequisites

Before building from source, make sure to install the following prerequisites:

Install [Rust](https://www.rust-lang.org/tools/install)
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Install build dependencies:
```bash
sudo apt update
sudo apt install build-essential git libssl-dev zlib1g-dev pkg-config clang jq
```

## How To Install

**Clone and build the node:**
```bash
git clone https://github.com/broxus/tycho
cd tycho
git checkout tags/0.0.0 -b mynode
cargo install --path ./cli
```

> [!NOTE]
> Use the latest tag for `git checkout`

**Generate node keys and configs**

```bash
# --global-config: file path or URL to the network global config
# --stake: stake value per round
tycho init --systemd --global-config ~/global-config.json --validator --stake 30000
```

**Backup keys**

- `~/.tycho/node_keys.json` - node keys which are used for the network and validation stuff;
- `~/.tycho/elections.json` - keys and settings of the validator wallet;

**Configure `~/.tycho/config.json`**

- `port` - node will listen on this UDP port;
- `storage.root_dir` - DB path, you might want to move it to a separate disk partition;
- `metrics.listen_addr` - Prometheus exporter will listen on this address;
- `rpc` - set it to `null` to disable an RPC endpoint if you want to save some disk space;

## Run validator node

- Enable `tycho` services:
  ```
  systemctl enable tycho --user --now
  systemctl enable tycho-elect --user --now
  ```

- Wait until the node is synced;
  > You can check it in metrics or logs. The control server currently doesn't return the sync status.
  > It will be added soon ([#395](https://github.com/broxus/tycho/issues/395)).

- Send testnet tokens to the validator wallet. The required amount is `10 + 2 * stake`.
> [!NOTE]
> You can find the wallet address in `~/.tycho/elections.json`.

## Resync validator node

> In case of major updates or fatal failures.

- Stop node services:
  ```bash
  systemctl stop tycho --user
  systemctl stop tycho-elect --user
  ```

- Delete node database:
  ```bash
  # Default db path, can be changed in ~/.tycho/config.json
  rm -rf ~/.tycho/db
  ```

- Start services:
  ```bash
  systemctl start tycho --user
  systemctl start tycho-elect --user
  ```
