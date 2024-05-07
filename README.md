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
make -j 10 static_lib
export ROCKSDB_LIB_DIR=/path/to/rocksdb
```

