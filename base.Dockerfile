FROM fedora:41

ARG ROCKSDB_COMMIT=ae8fb3e5000e46d8d4c9dbf3a36019c0aaceebff

RUN dnf install -y \
    git clang lld gcc-c++ make cmake which gflags-devel snappy-devel zlib-devel bzip2-devel libzstd-devel lz4-devel jemalloc-devel \
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
    make -j$(nproc) rocksdb && cp librocksdb.a /usr/lib64/librocksdb.a

ENV ROCKSDB_LIB_DIR /usr/lib64