# Auth validation measurements

Measured on macOS 27.0, Apple M3 Max (arm64), Rust 1.98.1, optimized Cargo bench
profile, with Criterion's default 100 samples / 3-second warmup / 5-second
measurement per benchmark. These are declared Criterion defaults, not runtime
polling or timeout policies.

`validation.rs` uses a unique SHM fleet, one session, one calling thread and
fixed fixture credentials/times. It unlinks only its own auth segment. These
measurements describe an uncontended warm path, not HTTP throughput or a loaded
multi-worker fleet. Cold checks deliberately clear the local views outside the
timed routine and include keyed lookup, subject verification, record MAC
verification and insertion of the local session view.

## Typed protection modes

The current `FleetAuth<P>` compares explicitly selected keyed BLAKE3,
XChaCha20-Poly1305 encrypted control records, and explicitly unprotected records.
All three keep token AEAD and live session/refresh/replay checks. Protected modes
reuse authentication on a warm hit only when every encoded bank byte and the
revision match the private snapshot. Unprotected mode trusts the revision alone
and therefore trusts all writers to follow the commit protocol.

Run all current modes with:

```sh
cargo bench -p orbit-auth --bench validation -- --save-baseline typed-state-final
```

Results use `auth_*`, `auth_encrypted_*` and `auth_unprotected_*` directories
under `target/criterion`, respectively. The final run used fresh opaque tokens
of 494, 498 and 500 bytes. Random IDs are serialized inside the encrypted token,
so encoded lengths can differ. Full-request and token-control measurements
include those fixture differences as well as ordinary run-to-run variation;
they do not isolate the cost of a MAC primitive. Token fingerprints and lookup/
subject identifiers still use HMAC-SHA256. Only record authentication changed
to keyed BLAKE3.

The encrypted mode here uses a fixed 96-byte control record and a local verified
snapshot. It does not restore the earlier serialized-state implementation or
decrypt shared state on every warm hit. No mode stores Principals in SHM.

| Operation | BLAKE3 | Encrypted | Unprotected |
| --- | ---: | ---: | ---: |
| Warm live-session check | 25.158 ns [25.045, 25.289] | 24.731 ns [24.621, 24.839] | 18.576 ns [18.443, 18.723] |
| Cached request including live session | 1.575 µs [1.571, 1.581] | 1.601 µs [1.595, 1.608] | 1.579 µs [1.575, 1.582] |
| Cold live-session check | 1.751 µs [1.749, 1.754] | 1.941 µs [1.935, 1.947] | 1.490 µs [1.485, 1.497] |
| Token decrypt + claims decode (control) | 2.100 µs [2.090, 2.109] | 2.173 µs [2.160, 2.188] | 2.110 µs [2.104, 2.116] |

Brackets are 95% confidence intervals. Compared with the previous HMAC-only
cold session check (2.259 µs), the BLAKE3 check measured 1.751 µs
(22.5% lower). This compares two implementations/runs, not just the primitive.
The protected warm paths remain around 25 ns because no MAC/decrypt occurs on
an unchanged snapshot; revision-only unprotected checks are around 19 ns.
Complete cached requests remain around 1.6 µs for all three policies.


Current wire layout is OAUTH004, kind 198, with two 136-byte banks in each
320-byte slot. Earlier kinds 199 and 255 are retained without mutation. Every
participant in a physical table must select the same policy.

## Earlier measurements

These historical designs are retained for context:

- `encrypted-state`: previously saved measurement of the encrypted/serialized
  shared state implementation; retained Criterion results, not rebuilt for this
  comparison.
- `atomic-state`: fresh measurement immediately before the MAC change; only the
  shared revision is read on a warm local hit.
- `mac-state-final`: MAC-authenticated fixed records; a warm hit compares every
  field, tag and revision with a previously verified local copy. A cold or
  changed record is authenticated using HMAC-SHA256. No token/Principal payload
  is placed in SHM.

The HMAC-only revision was measured with:

```sh
cargo bench -p orbit-auth --bench validation -- --save-baseline mac-state-final
```

Criterion estimates and confidence intervals are stored beneath
`target/criterion/auth_*/<baseline>/estimates.json`. The comparison below uses
Criterion slope estimates where available, otherwise the mean. It is a local
measurement, not a promised performance bound.

| Operation | Earlier encrypted state | Revision-only state | MAC state |
| --- | ---: | ---: | ---: |
| Warm live-session check | 18.512 µs [18.481, 18.547] | 17.558 ns [17.510, 17.605] | 24.507 ns [24.453, 24.571] |
| Cached request including live session | 20.034 µs [19.986, 20.091] | 1.558 µs [1.555, 1.561] | 1.579 µs [1.576, 1.582] |
| Token decrypt + claims decode (control) | 2.126 µs [2.112, 2.141] | 2.138 µs [2.133, 2.145] | 2.086 µs [2.081, 2.091] |
| Cold live-session check (local clear excluded) | Not measured | Not measured | 2.259 µs [2.256, 2.263] |

Brackets show Criterion's 95% confidence intervals. The final warm request
estimate is 1.31% above the fresh
revision-only baseline (20.4 ns), and
12.69 times faster than the retained
encrypted-state measurement. The isolated warm state check adds
6.9 ns. These differences compare whole
implementations, not the isolated cost of a cryptographic primitive; the token
control also shows ordinary run-to-run variation.


The MAC layout uses 320-byte slots rather than 256-byte slots: at the default
1,024 entries, the table grows from 256 KiB + 64 bytes to 320 KiB + 64 bytes.
That shape took kind 199; retained kind 255 prototypes are not resized or
migrated. The local session snapshot gains a 32-byte tag.

The guarantee is integrity against SHM writers that lack the state secret:
changed fields, forged revisions, changed tags and cross-slot copies are
rejected. A separate keyless process test exercises actual SHM tampering against
both warm and cold validation. MAC authentication does not stop restoring an
older genuine record and revision, deleting replay history, or denial of
service. A dedicated regression test records that rollback boundary.
