FROM fedora:42

ARG ROCKSDB_COMMIT="v10.4.2"

RUN dnf install -y \
    git clang lld gcc-c++ make cmake which gflags-devel zlib-devel bzip2-devel libzstd-devel lz4-devel jemalloc-devel \
 && dnf clean all

WORKDIR /build

# from rocksdb CMakeLists.txt:
# PORTABLE - Minimum CPU arch to support, or 0 = current CPU, 1 = baseline CPU

# Clone RocksDB and checkout the commit or a specific tag
RUN git clone https://github.com/facebook/rocksdb.git && \
    cd rocksdb && \
    git checkout ${ROCKSDB_COMMIT} && \
    mkdir build && cd build && \
    export CC=/usr/bin/clang && \
    export CXX=/usr/bin/clang++ && \
    cmake -DPORTABLE=1 \
          -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_CXX_FLAGS="-Wno-nontrivial-memcall" \
          -DWITH_LZ4=ON \
          -DWITH_ZSTD=ON \
          -DWITH_JEMALLOC=ON \
          .. && \
    make -j$(nproc) rocksdb

# used by https://crates.io/crates/rocksdb to link with librocksdb.a
ENV ROCKSDB_LIB_DIR="/build/rocksdb/build"
ENV ROCKSDB_STATIC=1
# otherwise will not build tycho image with provided librocksdb.a
ENV RUSTFLAGS="-l dylib=stdc++"
