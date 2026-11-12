#!/usr/bin/env bash
# Bash 3.2+; the one place this repository chooses a Cargo selection and its
# environment, so every caller reuses the same build artifacts.
set -eo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/check.sh [OPTIONS] [FILTER] [-- TEST_OPTIONS...]

Compile-check by default: cargo check with all features and targets, which is
what a change needs between steps. --format and --test run the slower stages.
  -p, --package NAME   Select this crate (repeatable). Without -p the
                       selection is the whole workspace.
  --format             Format the selection, then lint it with Clippy (all
                       features and targets; warnings are errors).
  --check              With --format: check formatting without changing files.
  --fix                With --format: run cargo fix on the selection first.
  --test               Test the selection with all features.
  FILTER               With --test: test-name substring.
  -- TEST_OPTIONS      With --test: options for the Rust test harness.
  -h, --help           Show this help.

scripts/format.sh is check.sh --format and scripts/test.sh is check.sh --test.

Examples:
  scripts/check.sh -p orbit-cache
  scripts/format.sh -p orbit-cache
  scripts/test.sh -p orbit-cache eviction -- --nocapture

Tests keep normal parallelism and do not enable ignored tests.
If scripts/env.local.sh exists it is sourced first, for this host's own
environment (a Cargo target directory, for one); it is never committed.
Uses the installed Cargo, rustfmt and Clippy; installs nothing.
EOF
}

fail() { echo "error: $*" >&2; exit 2; }

packages=()
filter=()
test_args=()
mode=check
check=0
fix=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p | --package)
            [[ -n "${2:-}" && "$2" != -* ]] || fail "$1 requires a package name"
            packages+=(--package "$2")
            shift 2 ;;
        --format)
            [[ "$mode" == check ]] || fail "--format and --test cannot be combined"
            mode=format; shift ;;
        --test)
            [[ "$mode" == check ]] || fail "--format and --test cannot be combined"
            mode=test; shift ;;
        --check) check=1; shift ;;
        --fix) fix=1; shift ;;
        -h | --help) usage; exit 0 ;;
        --)
            shift
            test_args=(-- "$@")
            break ;;
        -*) fail "unknown argument: $1 (see scripts/check.sh --help)" ;;
        *)
            [[ ${#filter[@]} -eq 0 ]] || fail "only one test-name filter is accepted"
            filter=("$1")
            shift ;;
    esac
done
[[ "$mode" == format || ( "$check" == 0 && "$fix" == 0 ) ]] \
    || fail "--check and --fix belong to --format"
[[ "$check" == 0 || "$fix" == 0 ]] || fail "--check and --fix cannot be combined"
[[ "$mode" == test || ( ${#filter[@]} -eq 0 && ${#test_args[@]} -eq 0 ) ]] \
    || fail "a test filter or test options belong to --test"
command -v cargo >/dev/null 2>&1 || fail "Cargo is required; install Rust with rustfmt and Clippy"

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$project_dir"
if [[ -f scripts/env.local.sh ]]; then
    # shellcheck source=/dev/null
    source scripts/env.local.sh
fi

selection=(--workspace)
fmt_scope=(--all)
scope_name='workspace'
if [[ ${#packages[@]} -gt 0 ]]; then
    selection=("${packages[@]}")
    fmt_scope=("${packages[@]}")
    scope_name='selected packages'
fi

case "$mode" in
    check)
        echo "Running cargo check ($scope_name, all features and targets)..."
        cargo check "${selection[@]}" --all-features --all-targets
        ;;
    format)
        if [[ "$fix" == 1 ]]; then
            echo "Running cargo fix ($scope_name, all features and targets)..."
            cargo fix "${selection[@]}" --all-features --all-targets --allow-dirty --allow-staged
        fi
        fmt_args=()
        [[ "$check" == 0 ]] || fmt_args=(-- --check)
        echo "Running rustfmt ($scope_name)..."
        cargo fmt "${fmt_scope[@]}" "${fmt_args[@]}"
        echo "Running Clippy ($scope_name, all features and targets; warnings are errors)..."
        cargo clippy "${selection[@]}" --all-features --all-targets -- -D warnings
        ;;
    test)
        echo "Running tests ($scope_name, all features, normal test parallelism)..."
        cargo test "${selection[@]}" --all-features --no-fail-fast "${filter[@]}" "${test_args[@]}"
        ;;
esac
echo 'Checks passed.'
