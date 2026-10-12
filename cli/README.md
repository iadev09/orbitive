# Orbitive CLI

`orbitive-cli` is the command-line tool distributed by
[Orbitive](https://github.com/iadev09/orbitive). It inspects and removes Orbit
POSIX shared-memory objects. Cache exploration maps existing mutation history;
selected values use the arena's existing reader pins. The installed command is
`orbit`.

## Install

```sh
cargo install orbitive-cli
```

## Usage

Discover fleets without knowing their names:

```sh
orbit fleets
orbit fleets --uid 501
```

Each row shows the fleet name, segment count and total segment size. Discovery
uses existing fleet membership and segment companion lock filenames as hints,
then confirms exact SHM names with `shm_open`. Stale locks without SHM are
excluded. This is a snapshot of retained resources, not a running-process list.
No registry is created and no fleet is joined. Objects without a companion
lock hint, or outside the selected lock directory, cannot be discovered.

List every shared-memory kind for a known fleet owned by the current effective user:

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

## Cache explorer

The explorer is read-only. It exposes no put, delete or reset operation and
never changes cache values. Selected reads only update existing ownership/pin
counters for memory safety; they do not publish cache mutations.

```sh
orbit cache example
orbit cache example --store models
orbit cache example --store models --key 'user:42'
orbit cache example --store models --key 'user:42' --format hex
orbit cache example --json
```

Rows show store, key, remaining TTL in milliseconds, byte size and status.
No expiry means `none`; expired candidates have TTL zero and their values are
not opened. Values are read only with an explicit `--store` and `--key`.
Presentation defaults to JSON, then UTF-8 text, then hex. `--format` chooses
`json`, `text` or `hex` explicitly; it never writes converted bytes back.
The CLI cannot infer a binary application's schema or archived Rust types.
Terminal control characters are escaped. JSON output contains the same
metadata, exact store/key bytes in hex, and the selected value if requested.

**This is retained history, not a full cache snapshot.** The cache's key
metadata is process-local; there is no authoritative shared key index.
The explorer applies the retained Put/Delete/Reset window in revision order,
reports overwritten/unavailable/malformed/pending frames and concurrent
changes, and always marks JSON output `authoritative: false`. A missing key
does not prove a miss in a running worker. Candidates may be absent from L1;
an arena generation that was reused reports `payload_unavailable` rather than
replacement bytes. No application backing store is queried.

Listing is read-only: it claims no node, pin or membership and creates no SHM.
Selected value reads briefly claim one existing arena reader-owner row and
pin only the descriptor's exact generation, copy the body, then drop the pin
before decoding/output. No arena writer lock, timeout, polling, extra index,
or layout change is introduced. A full reader table fails explicitly.
Pinning requires the owner's uid. The reader validates the arena ABI and
derives capacities from its existing header, not the CLI's compiled profile.
`--uid` can inspect another user's metadata if OS
permissions allow; for their values, execute the CLI as that user.

For remote inspection, run the same command on the owning machine via SSH:

```sh
ssh example-host orbit cache example --json
```

## Clearing safety

Only clear a stopped fleet. POSIX unlink removes the shared-memory name but
does not invalidate mappings already held by running processes. A later
process can therefore create a new object under the same name while existing
members still use the old mapping.

The command constructs and probes only exact
`/orbit-{fleet}-{kind}-{uid}` names across the 256 Orbit kind values. It does
not use an `orbit-*` glob, so similarly prefixed objects are never listed or
removed. Probing through `shm_open` also works without a `/dev/shm` filesystem
view. The CLI currently supports Unix targets.
