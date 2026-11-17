# Authentication boundary

The crate is a transport-neutral trust primitive. Authority configuration owns
realm/issuer labels and an immutable symmetric keyring. Validation policy owns
an exact audience, purpose, and required capabilities. These values are trusted
configuration, not token-controlled routing hints. Hostnames and fleet membership
do not imply authentication or authorization.

## Envelope v1

`oa1.` + canonical unpadded base64url(`u32be kid | nonce[24] | AEAD ciphertext+tag`).
The plaintext is a strict JSON Principal. Time units are Unix seconds.
Envelope version is fixed; no algorithm negotiation or key lookup fallback exists.
Unknown keys, malformed input, context mismatch and failed tags are rejected.
Untrusted wire input is bounded before base64 allocation or decrypt.

HKDF-SHA256 salt is `orbit-auth/v1/hkdf-sha256`. Every context part is prefixed
with its u64 big-endian byte length. Encryption info is the tuple
(`orbit-auth/v1/xchacha20poly1305`, realm, issuer, audience, purpose, kid:u32be),
also used as AEAD associated data. XChaCha20-Poly1305 uses a fresh OS-random
192-bit nonce on each issue. JTI is a separately generated random 128-bit value.
The key and nonce algorithms are supplied by RustCrypto/getrandom, not implemented here.

The envelope is a private versioned format using established primitives, not a
claim of compatibility with KWT, JWT or PASETO. Any wire change requires a new
version and a deliberate migration policy.

## Cache invariants

Subject-wide session control reuses the authenticated session table and its
existing exclusive writer lock. A bounded admission inserts the new session
and revokes the oldest excess records of that realm/issuer/subject while holding
that lock. Failed admission does not revoke existing sessions. Bulk revocation
and extension authenticate occupied records before mutation; extension validates
all horizons first and never shortens or resurrects a session. Concurrent readers
observe individual committed records, not an all-record atomic snapshot.

There is no extra subject index, ring, background loop, invented lifetime, or
shared-memory layout change. Subject scans are bounded by table capacity and are
control/activity operations; ordinary token validation retains direct keyed
lookup. The caller owns notifications and explicit extension policy.

Cache namespace seed hashes sorted `(kid, derived_cache_subkey)` pairs. Cache
subkey info is (`cache`, realm, issuer, kid). The namespace therefore changes
when any retained key is added, removed or replaced, but not when only the active
issuing key changes. A prepared validator derives a policy HMAC key over the
audience, purpose and required capability set. HMAC of the original token bytes
produces the final 32-byte lookup key. No raw credential is resident in the cache.

Only successfully validated and hook-approved Principals enter the typed cache.
Entries carry their lookup key, creation time and absolute expiry, bounded by
both token expiry and explicit caller TTL. Cache hits verify identity, time,
capabilities and the current hook. A shortened TTL is applied even to older
entries; rollback before entry creation forces token revalidation. Hooks are not
cached, including their rejection/unavailability decisions.

Concurrent cold misses may decrypt independently. `FleetAuth` implements both
`PrincipalCache` and live session hooks using its own current-state table over
core. Backing follows the supplied Fleet, never the application's run mode.
Memory handles sharing one Arc<Fleet> use a weak registry; SHM uses kind 198.

Principal payloads never enter SHM. Each worker validates the token on its first
local miss and retains the decoded Principal. A second local LRU maps SessionId
to an authenticated metadata snapshot, its complete encoded bank, slot index and revision.
With either protected policy, validation copies the selected bank between two
atomic revision reads. A warm hit compares every encoded word and revision with
the trusted local copy. Exact equality reuses prior authentication/decryption;
revision alone would miss a hostile writer modifying fields without a commit.
Changed records are reloaded and authenticated before use. The explicitly
unprotected policy instead trusts the writer's revision protocol. Time and policy
checks always run. No event delay or configurable revocation TTL exists.

SHM contains HMAC-derived lookup and subject identifiers plus fixed control
words: created_at, expires_at, refresh generation, status. No subject text,
claims, token or secret key is present. `FleetAuth<P>` has sealed protection
policies: `Blake3State` (ID 1), `EncryptedState` (ID 2), and explicitly
opted-in `UnprotectedState` (ID 3). Every constructor requires a policy marker;
there is no generic default or implicit constructor. Token encryption, validation and live checks
remain enabled in every mode. The physical table records the policy ID; a
mismatched open fails without mutation, including the memory registry path.
The type supplies the expected policy; untrusted metadata never selects it.

The state key is separately provisioned and stable across token-key rotations.
Identifier subkeys remain scoped to fleet/realm/issuer. Protection uses HKDF info
(`orbit-auth/v4/fleet-state-protection`, fleet name, policy ID:u64le). It is
fleet-scoped so distinct realm/issuer namespaces can share one table; participants
share the root state secret. Keyed record identifiers bind the logical auth domain.

Authentication context is `orbit-auth/v4/state-record`, kind byte, u64le policy
ID, u64le slot index and u64le revision. Blake3State appends the 96-byte record,
computes a full keyed BLAKE3 tag and compares `blake3::Hash` values in constant
time. Its bank is record[96] + tag[32] + zero padding[8]. EncryptedState uses
that context as AAD, an OS-random 24-byte nonce per write and XChaCha20-Poly1305.
Its bank is nonce[24] + encrypted record[96] + tag[16]. Decryption uses a zeroized
stack buffer before shape validation; no JSON or Principal serialization occurs.
UnprotectedState is record[96] + zero padding[40], with shape checks only.
Its explicit contract trusts all SHM writers; it cannot claim record integrity.

In protected modes, every occupied probe record is verified before key/expiry
can authorize absence or reuse. Different keys, field changes, slot transplants
and revision changes fail verification. The two protected modes compare the complete bank on
a warm hit. This is an equality check against previously
verified local ciphertext/MAC bytes, not a comparison of a freshly computed tag.
Changed content requires constant-time cryptographic verification again.
UnprotectedState instead compares only the commit revision on a warm hit and
trusts all writers to publish revisions correctly; a hostile in-place mutation
may remain invisible to its warm cache until a committed change or eviction.

In the two protected modes, a keyless same-UID SHM writer cannot forge new accepted state. Authentication
cannot prevent restoring a previous genuine record/revision, deleting records,
unlinking/truncating the mapping or denying service. In particular, deletion
or rollback of consumed IDs can bypass one-use history. A valid tag proves
origin/integrity, not currentness. OS isolation and trusted lifecycle ownership
remain required; key-holding compromised runtimes can forge state. Tests pin
both the tamper rejection and the explicit rollback limitation.

Each physical slot is 320 bytes: one non-wrapping u64 revision and two banks of
17 atomic u64 words (136 encoded bytes). A writer holds the kernel flock, fills
the inactive bank and publishes the next revision. All operations are SeqCst;
a reader racing two writers cannot accept mixed fields even if they return to
the same bank. A raced snapshot causes one locked read, never a spin loop.
Authority mutation and replay admission retain writer locking and atomic commit.
A pre-commit crash leaves the previous bank and its tag intact. Revision
exhaustion fails closed; cooperating writers never wrap or reset revisions.
Hostile writers can violate that protocol, so its revision is not a freshness
anchor against rollback.

The table uses bounded linear probing. Slots are only replaced by another
committed record after expiry, never cleared into probe-chain holes. Cached
references cannot authorize a replacement session because revisions change on
reuse. No live record is evicted. Missing sessions and malformed metadata deny.
Session refresh changes generation and may extend the caller-owned horizon;
access credentials ignore refresh generation but retain expiry and revocation.
Session extension moves only the horizon, forward, for the subject whose keyed
tag matches the record; an equal horizon commits nothing, so warm views stay
valid. Credentials still carry their own expiry bounded by the horizon at
issue, so a longer life is a new credential from the extended handle.

The typed layout OAUTH004 uses kind 198 and 320-byte slots. The header stores
the policy at bytes 40..48. OAUTH003 kind 199 remains reserved for the prior
HMAC-only layout with 16-word banks; kind 255 retains OAUTH001/OAUTH002 prototypes.
The 17-word bank stride requires a new kind even though slot size stays 320.
No retained segment is resized, reinterpreted, reset or unlinked. The canonical
downstream index records the allocations. Transitioning to an empty table
requires retiring outstanding replay-protected credentials as well as session
reauthentication, or a separately designed owner-managed migration.

Auth currently requires owner-only 0600 SHM. Core's group access policies do
not yet supply writable cross-UID attach/common locking: writer locks live in
UID-specific private directories and writable attach validates the current UID.
A future 0660 encrypted deployment needs both explicit trusted owner/group
configuration and the same securely provisioned writer lock for every participant.
Encryption alone cannot fix separate locks or supply rollback resistance.

SHM drop/restart preserves state; handles retain Fleet membership. No driver,
poll interval, automatic reset/unlink or downstream runtime dependency exists.

## Caller ownership

The caller supplies trusted current time and all lifetimes. The caller replaces
authority/validator snapshots on key rotation; old live validators still retain
old trust. Connection-bound Principals need caller-owned expiry/re-authentication
and revocation checks. Issuance callers must enforce delegation limits themselves.
CSRF protection, cross-origin policy, parsing ambiguous/multiple credentials,
WebSocket handshake conventions, authentication deadlines, and operation admission
remain above this crate.

## Owned validators

`Authority::validator` still returns a borrowed `Validator<'_>`. The validator
borrows only realm/issuer labels; it already owns its prepared policy and
zeroized derived secrets. `into_owned()` moves that state unchanged and owns
the labels, yielding `OwnedValidator` (an alias for `Validator<'static>`).
It does not retain the root keyring, rederive keys, or change the cache namespace.
Both forms use the same validation implementation and envelope context helper.
A retained owned validator preserves its original trust snapshot after issuer
drop/rotation; applications replace it when their acceptance policy changes.
