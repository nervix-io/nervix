# syntax=docker/dockerfile:1.7

ARG DEBIAN_IMAGE
FROM ${DEBIAN_IMAGE}

ARG LLVM_KEY_SHA256

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        binutils ca-certificates cmake curl g++ g++-aarch64-linux-gnu \
        git gnupg make ninja-build patch python3 python3-tqdm qemu-user \
    && curl --proto '=https' --tlsv1.2 -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key -o /tmp/llvm.key \
    && echo "${LLVM_KEY_SHA256}  /tmp/llvm.key" | sha256sum --check - \
    && gpg --dearmor -o /usr/share/keyrings/llvm.gpg /tmp/llvm.key \
    && printf 'deb [signed-by=/usr/share/keyrings/llvm.gpg] https://apt.llvm.org/trixie/ llvm-toolchain-trixie-%s main\n' 23 21 >/etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        clang-23 lld-23 llvm-23 clang-21 \
    && ln -s /usr/bin/ld.lld-23 /usr/local/bin/ld.lld \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.key

ENV CC=clang-23
ENV CXX=clang++-23
ENV AR=llvm-ar-23
ENV CUDAHOSTCXX=clang++-21

WORKDIR /workspace