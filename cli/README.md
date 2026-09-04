# Orbitive CLI

`orbitive-cli` inspects and removes Orbit POSIX shared-memory objects without
mapping their data structures or joining a fleet. The installed command is
`orbit`.

## Install

```sh
cargo install orbitive-cli
```

## Usage

List every shared-memory kind owned by the current effective user:

```sh
orbit list example
```

Inspect or remove one kind:

```sh
orbit list example --kind 231
orbit clear example --kind 231
```

Remove every discovered kind for a fleet:

```sh
orbit clear example --all --yes
```

Pass `--uid UID` when inspecting objects owned under a different effective
user id. Run `orbit help` or `orbit help <COMMAND>` for the complete command
reference.

## Safety

Only clear a stopped fleet. POSIX unlink removes the shared-memory name but
does not invalidate mappings already held by running processes. A later
process can therefore create a new object under the same name while existing
members still use the old mapping.

The command constructs and probes only exact
`/orbit-{fleet}-{kind}-{uid}` names across the 256 Orbit kind values. It does
not use an `orbit-*` glob, so similarly prefixed objects are never listed or
removed. Probing through `shm_open` also works without a `/dev/shm` filesystem
view. The CLI currently supports Unix targets.
