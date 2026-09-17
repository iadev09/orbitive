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

# Data races in the shared-memory substrate, found rather than reasoned about.
#
# This crate's `unsafe` is mostly one thing: several processes and several
# threads reading and writing the same mapped bytes. `cargo test` cannot tell a
# correct seqlock from an undefined one, because both work on the hardware we
# run on — the 2026-09-12 ring bug passed the whole suite for as long as it
# existed. ThreadSanitizer can, and does: run against that ring it reports
# "data race … in ShmRing::write_slot" from
# `a_reader_and_a_writer_contend_for_one_slot`.
#
# Needs nightly with `rust-src`: the sanitizer has to instrument std as well,
# or a race that passes through it is invisible. Doctests are excluded because
# rustdoc builds them without the sanitizer and then refuses the ABI mismatch.
#
# `halt_on_error=1` because a TSan warning does not fail a test by itself, and
# a report nobody's exit code notices is not a check.
race:
    TSAN_OPTIONS=halt_on_error=1 \
    RUSTFLAGS=-Zsanitizer=thread \
    cargo +nightly test -p orbit-core -p orbit-lock -p orbit-stream \
        --target "$(rustc -vV | awk '/^host:/{print $2}')" \
        -Zbuild-std --lib --tests -- --test-threads=1
