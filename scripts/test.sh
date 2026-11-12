#!/usr/bin/env bash
# Test through scripts/check.sh, which owns the Cargo selection.
exec "$(dirname -- "${BASH_SOURCE[0]}")/check.sh" --test "$@"
