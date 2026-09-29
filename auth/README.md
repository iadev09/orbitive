# orbit-auth

Fleet-aware authentication with encrypted tokens, shared session state and local
Principal caching. Available through `orbitive::auth` with the `auth` feature
(also included in `full`), or as a direct dependency.

## Fleet-aware authentication

A user's requests may reach different workers. In an SHM-backed fleet, every
worker checks the same session, revocation and refresh state while keeping its
own decoded Principal cache. A session created on one worker is recognized by
the others when they validate its credentials; logout applies to subsequent
checks across the fleet. Concurrent refresh attempts are coordinated atomically.
Authentication needs no sticky-session routing or background state polling.

## Optional: cross-application sign-in and events

Fleet members on one host can serve different applications—a web portal, an API
and a realtime service—while accepting credentials linked to one shared session.
Users can carry their sign-in between applications, and one logout revokes all
linked tokens. The application handles credential delivery across domains.

Downstream can combine this with [orbit-events](../events/README.md) to notify
other members and their connected clients. For example, publish application-defined
events after the auth operation succeeds:

```text
Sign in → create session + issue token → emit TokenCreated { session_ref }
Logout  → revoke session              → emit SessionInvalidated { session_ref }
```

Subscribers can prompt associated clients to obtain credentials or close a
revoked connection. A session reference is enough; no token or user payload needs
to be broadcast. Events notify, credentials authenticate: receiving an event does
not sign a client in. Shared-state checks still enforce logout if an event is missed.

## Usage

Choose the shared-state protection explicitly:

```rust
use std::{num::NonZeroUsize, sync::Arc};
use orbit_auth::{Authority, Blake3State, FleetAuth, Keyring, SecretKey};

// Demo keys. Provision the same token and state keys to participating workers.
let authority = Authority::new("prod", "login",
    Keyring::new(1, [(1, SecretKey::generate()?)])?)?;
let fleet = Arc::new(orbit_core::Fleet::join("example", 1).unwrap());
let auth = FleetAuth::new(
    fleet, &authority, SecretKey::generate()?,
    NonZeroUsize::new(1024).unwrap(), Blake3State,
)?;
# Ok::<(), orbit_auth::Error>(())
```

Create a session with `create_session`, issue its token with
`Authority::issue_session`, then use `validate_cached` with `&auth` as both
cache and validation hook. See the [complete example](examples/transports.rs).

## State protection

| Policy | SHM record protection |
| --- | --- |
| `Blake3State` | Keyed BLAKE3 authentication; metadata remains readable |
| `EncryptedState` | XChaCha20-Poly1305 encryption and authentication |
| `UnprotectedState` | No cryptographic protection; trusts every writer |

There is no default. Peers opening the same table must select the same policy.
**Tokens are always encrypted and authenticated**, independently of this choice.

## Shared state and local cache

Backing follows the supplied Fleet: `join` uses memory; `join_shm_as` uses
same-host SHM. Memory handles share state through the same `Arc<Fleet>`; SHM
peers share a fleet name, UID, protection policy and state key.

SHM holds hashed identifiers, expiry, refresh generation and revocation/replay
status. Tokens and decoded Principals stay out of SHM. Each worker validates a
token on a local cache miss. Cache hits still check expiry and live session state;
unchanged protected records reuse local verification without repeating crypto.

- `revoke_session` rejects linked tokens on subsequent checks, including cache hits.
- `refresh` consumes one refresh generation atomically. Existing access tokens
  remain valid until their own expiry or session revocation.
- `replay_guard` admits a token's `jti` once. Full tables reject new state rather
  than evict live records.

## Integration and limits

Adapters extract credentials from cookies, Bearer headers, WebSocket auth frames
or internal handshakes. They own TLS, cookie/CSRF/CORS policy and connection expiry.
Shared trust can span domains; cookies cannot span different registrable domains.

- Tokens use a versioned `oa1.` envelope with XChaCha20-Poly1305 and HKDF-SHA256.
  Validate the expected audience, purpose and capabilities. All key holders can issue tokens.
- Use `FleetAuth` as the hook for session-bound tokens; `AllowReusable` skips session checks.
- Supply time and lifetimes explicitly. Cache TTL never extends token expiry.
  Keep the state key stable across token-key rotation.
- SHM requires `0600`. Protected modes reject forged records but cannot prevent
  restoring an older valid record, deleting state or denying service.
- SHM survives restart. Clearing it loses sessions and replay history; retire
  outstanding one-use credentials before clearing. The table defaults to 1024
  entries, configurable at build time with `ORBIT_AUTH_STATE_CAPACITY`.

[Performance measurements](benches/README.md)
