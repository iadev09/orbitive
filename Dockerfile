FROM rust:1.95-bookworm

WORKDIR /workspace

COPY . .

# Compile the Linux-only eventfd/futex path while building the image. The
# container entrypoint then executes the already-built Orbit test suite.
RUN cargo test --locked -p orbit-core --all-targets --no-run

CMD ["cargo", "test", "--locked", "-p", "orbit-core", "--all-targets"]
