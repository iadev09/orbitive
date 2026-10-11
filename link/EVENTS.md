# Link events: carrying fleet events between fleets (design)

Status: design, not implemented. This note fixes the shape before code.

## What it is for

A fleet's event ring (`orbit-events`) reaches every process of that fleet and
no one else. Two fleets that meet in a link segment — an edge and an
application, each with its own fleet — have no path for an event published in
one to reach the other. This feature carries selected events across that
segment: **ring → link → ring**.

It is an API, never automatic. Nothing is bridged because two features happen
to be enabled together; a downstream composes it explicitly:

```toml
orbitive = { version = "…", features = ["link-events"] }   # = ["link", "events"]
```

Enabling `link` and `events` separately does not pull it in. The downstream
says "I set up the link, and I bind events to it" in code, choosing which
segment, which ring and when.

## Model

An event has **one origin**: one process published it once. That decides
every rule below.

- **Forwarded once, at the origin.** Only the process that published an event
  forwards it. Every process of the fleet reads the ring; if each forwarded
  what it read, one event would cross once per process. `FleetEvent::id` is a
  `NetId64` whose node is the publisher's, so the rule is checkable here:
  forward an event only when `id.node()` is this process's own node.
- **Received by one, published once.** The forwarder writes an event frame to
  one lane of the other fleet, chosen the way a caller chooses a request
  target. Only that lane's owner reads it, because a lane is a single-reader
  mailbox. It publishes the event into its own fleet's ring once; from there
  every process of that fleet sees it once.
- **Never back where it came from.** A received event is republished by the
  receiver, so its new id carries the receiver's node and would pass the
  origin rule. The receiver remembers the ids it republished together with
  the segment each came from, and never forwards one of them back to that
  segment. The memory is exact (ids it wrote itself), local, and released
  when its own forwarder has read past the id. Forwarding to a *third*
  segment stays allowed, which is what lets a hub relay between leaves. A
  hub-and-leaves topology (one edge, its applications) has no cycle to guard
  against beyond that.
- **Interest decides, not configuration.** An event crosses only to a fleet
  where some process wants it. Interest is current state in the link table:
  each lane publishes the routing keys its process subscribes to. A subscriber
  declares interest simply by being able to decode the event; the forwarder
  reads the other side's lanes and skips keys nobody there wants.
- **The routing key is the caller's.** For a plain event the key is its ring
  topic. An envelope that carries many domain topics under one ring topic —
  an application-event envelope, for instance, where every event shares one
  ring topic and the domain topic sits inside the payload — would otherwise
  collapse every event into one interest. Such a downstream decodes its
  envelope and supplies the inner topic as the key. The feature never opens a
  payload; a key is bytes to it.

## Pieces

**Frame tag.** The link core defines no application frame, and still does
not: the event frame belongs to this feature. Frames share a lane with the
downstream's own frames, so the first byte is the discriminator. This feature
reserves the range `0xF0..=0xFF` for orbitive frames and uses one value in it
for an event; downstream frames keep `0x00..=0xEF`. A reader that finds a tag
in the reserved range hands the frame to this feature's decoder.

**Event frame.** Tag, topic length and bytes, payload length and bytes. The
topic and payload are exactly what `FleetEventBus::publish` takes; the
feature does not interpret either. It must fit one inbox slot; a larger event
is refused at the forwarder, not truncated.

**Interest.** Per lane, a fixed-size bit set of routing-key hashes (a Bloom filter)
in a slot of its own after the watcher table. A false positive only forwards
an event nobody consumes, and the receiver's own subscriptions drop it; a
fixed size avoids a topic count limit. Updating it bumps the directory
generation, so observers learn of it through the wake they already have.

**API sketch.**

```rust
// Bind a fleet's event ring to a link segment this process has joined.
let bridge = LinkEvents::bind(&segment, lane, &bus)?;

// Interest is in routing keys. For plain events the key is the ring topic;
// an envelope carrying many domain topics under one ring topic supplies the
// inner one.
bridge.interest(&["wallet.expired.v1"])?;

// Own-origin events only; the caller says which key this one routes by.
bridge.forward(&event, key)?;

// An event frame read from this lane: publish once into this fleet's ring.
bridge.accept(&frame)?;
```

The downstream drives it: its own ring cursor feeds `forward` with the key it
extracted, its own lane reader passes event frames to `accept`. The feature
owns no thread, task or interval, and does no typing: topics, keys and
payloads stay bytes.

## Open decisions

- Size of the interest set (bits per lane) and the hash; it is geometry, so it
  belongs in the link spec rather than a constant.
- Whether interest lives in the lane header's spare bytes (no growth) or in
  its own table (growth, so a new kind).
- How the receiver's republished-id memory is bounded if its forwarder stops
  reading (shutdown): released on drop, or capped by the ring's own capacity.
