FROM fedora:41

ARG ROCKSDB_COMMIT=410c5623195ecbe4699b9b5a5f622c7325cec6fe

RUN dnf install -y \
    git clang lld gcc-c++ make cmake which gflags-devel zlib-devel bzip2-devel libzstd-devel lz4-devel jemalloc-devel \
 && dnf clean all

WORKDIR /build

# Clone RocksDB and checkout the commit or a specific tag
RUN git clone https://github.com/facebook/rocksdb.git && \
    cd rocksdb && \
    git checkout ${ROCKSDB_COMMIT} && \
    mkdir build && cd build && \
    export CC=/usr/bin/clang && \
    export CXX=/usr/bin/clang++ && \
    cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
          -DCMAKE_BUILD_TYPE=Release \
          -DWITH_LZ4=ON \
          -DWITH_ZSTD=ON \
          -DWITH_JEMALLOC=ON \
          .. && \
    make -j$(nproc) install

ENV ROCKSDB_LIB_DIR /usr/lib64
