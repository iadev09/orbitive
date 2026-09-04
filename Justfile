set shell := ["bash", "--noprofile", "--norc", "-euo", "pipefail", "-c"]

default:
    @just --list

# Native smoke suite used by Linux/FreeBSD Gitea Actions runners.
smoke:
    uname -a
    rustc --version
    cargo --version
    cargo test --workspace --all-targets

smoke-linux:
    test "$(uname -s)" = "Linux"
    just smoke

smoke-freebsd:
    test "$(uname -s)" = "FreeBSD"
    just smoke
