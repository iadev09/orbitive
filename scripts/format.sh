#!/usr/bin/env bash
# Format and lint through scripts/check.sh, which owns the Cargo selection.
exec "$(dirname -- "${BASH_SOURCE[0]}")/check.sh" --format "$@"
