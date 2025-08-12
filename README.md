## Tycho

Reference implementation of Tycho protocol.

## About

Tycho is a high-performance protocol designed for building L1/L2 TVM blockchain
networks. By utilizing DAG (Directed Acyclic Graph) for consensus and TVM (TON Virtual Machine) for
parallel execution, Tycho works with high throughput and low latency.

- **[Testnet Validators Guide](./docs/validator.md)**
- **[CLI Reference](./docs/cli-reference.md)**

## Development

- Install Rust:
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- Install dependencies:
  ```bash
  sudo apt install build-essential git libssl-dev zlib1g-dev pkg-config clang jq
  ```
- Install tools:
  ```bash
  cargo install lychee cargo-shear cargo-nextest
  ```
- Test CI locally:
  ```bash
  just ci
  ```

## To run prebuilt tests

check [this](docs/testing.md) guide.

## Running a Local Network

```bash
# Generate zerostate config stub (with optional --force flag):
just init_zerostate_config
# Generate node config sub (with optional --force flag):
just init_node_config
# Generate a local network of 3 nodes (with optional --force flag):
just gen_network 3

# Start nodes in separate terminals or spawn them with `&`:
just node 1
just node 2
just node 3
```

> [!NOTE]
> By default the `dev` profile is used. Use this env to specify a different
> profile:
> ```bash
> export TYCHO_BUILD_PROFILE=release
> ```

## Prebuilt RocksDB

By default, we compile RocksDB (a C++ project) from source during the build.
By linking to a prebuilt copy of RocksDB this work can be avoided
entirely. This is a huge win, especially if you clean the `./target` directory
frequently.

To use a prebuilt RocksDB, set the `ROCKSDB_LIB_DIR` environment variable to
a location containing `librocksdb.a`:

```bash
export ROCKSDB_LIB_DIR=/usr/lib/
cargo build -p tycho-cli
```

Note, that the system must provide a recent version of the library which,
depending on which operating system you're using, may require installing
packages
from a testing branch. Or you could build the RocksDB from source manually:

```bash
# Install dependencies
sudo apt install clang lld libjemalloc-dev libgflags-dev libzstd-dev liblz4-dev

# Clone the repo
cd /path/to
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout v10.4.2

# Build the library
mkdir -p build && cd ./build
export CC=/usr/bin/clang
export CXX=/usr/bin/clang++
cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-Wno-nontrivial-memcall \
  -DWITH_LZ4=ON -DWITH_ZSTD=ON -DWITH_JEMALLOC=ON ..
make -j16 rocksdb rocksdb-shared

# Set env somewhere
export ROCKSDB_LIB_DIR=/path/to/rocksdb/build
```

> By default the manually built RocksDB will be linked dynamically.
> You can set `ROCKSDB_STATIC=1` to link the library statically.
> However, for debug builds everything compiles faster without it.

## Contributing

We welcome contributions to the project! If you notice any issues or errors,
feel free to open an issue or submit a pull request.

## AI Documentation
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/broxus/tycho)


## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or <https://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT)
  or <https://opensource.org/licenses/MIT>)

at your option.
