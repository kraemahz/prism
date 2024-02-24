# Define the Rust build stage
FROM rust:1.74 as builder
# Install Cap'n Proto dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    git \
    make \
    autoconf \
    libtool \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
# Install Cap'n Proto from source
RUN git clone https://github.com/capnproto/capnproto.git \
    && cd capnproto/c++ \
    && git checkout v1.0.2 \
    && autoreconf -i \
    && ./configure \
    && make -j$(nproc) \
    && make install
# Create a new empty shell project
RUN USER=root cargo new --bin prism
WORKDIR /prism
# Copy your source tree
COPY ./prism_server ./prism_server
COPY ./prism_schema ./prism_schema
COPY ./prism_client ./prism_client
COPY ./prism_python_client ./prism_python_client
COPY ./prism_util ./prism_util
COPY Cargo.toml Cargo.lock ./
# Build your application
RUN cargo build --release


#
### Define the final image
#
FROM debian:12-slim
# Install SSL certificates
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
# Copy the binary from the builder stage
COPY --from=builder /prism/target/release/prism_server /usr/local/bin/prism_server
# Set the default command to run the server
# Assuming the binary binds to localhost:5050 by default
CMD ["prism_server", "/opt/prism/conf.json"]
