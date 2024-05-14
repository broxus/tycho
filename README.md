# tycho

## Pre-requisites

- [rust](https://rustup.rs/)
- [just](https://pkgs.org/download/just)
- [zstd](https://pkgs.org/download/zstd)
- [clang](https://pkgs.org/download/clang)
- [llvm](https://pkgs.org/download/llvm)

## Testing

To run tests from ci:

```bash
just ci
```

To interactivly choose what to run:

```bash
just
```

To format code:

```bash
just fmt
```

## Local Network

```bash
# Generate zerostate config stub (with optional --force flag):
just init_zerostate_config
# Generate node config sub (with optional --force flag):
just init_node_config
# Generate a local network of 3 nodes:
just gen_network 3

# Start nodes in separate terminals or spawn them with `&`:
just node 1
just node 2
just node 3
```

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
depending on which operating system you're using, may require installing packages
from a testing branch. Or you could build the RocksDB from source manually:

```bash
cd /path/to
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout v8.10.0
mkdir -p build && cd ./build
cmake -DWITH_LZ4=ON -DWITH_ZSTD=ON -DWITH_JEMALLOC=ON -DCMAKE_BUILD_TYPE=Release ..
make -j16 rocksdb
export ROCKSDB_LIB_DIR=/path/to/rocksdb/build
```
