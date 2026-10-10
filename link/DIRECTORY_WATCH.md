# Directory watch: being woken is not being connected

This note records one change to `orbit-link` and the reasoning behind it. The
code comments say what each piece does; this file says why the shape changed.

## What was wrong

The lane directory — which lanes are claimed, under which name, role,
incarnation and pid — is current state, kept in the inbox table. Its
generation (`Inbox::directory()`) moves exactly when a lane is claimed or
released, never on traffic.

Since the directory wake was added, a change wakes every *claimed lane*: the
lane's `changes` word is bumped and, when its Tokio receiver is armed, its
named socket is rung. A process learns who joined or left from the lane it
already reads.

That tied two different things to one mechanism:

- **holding a lane** is serving: a process that takes frames under a name;
- **hearing that the directory moved** is observing.

Only the first could do the second. A process that serves nothing — a
master, whose workers hold the lanes — had no way to be woken. It could read
the table, but to notice a change it would have had to poll, and a poll is not
allowed on an Orbit path: it costs a wake per interval per waiter forever and
hides the missing signal instead of adding it.

The workaround that suggested itself was worse: let each worker turn what it
observed into a fleet event so the master hears it. Every worker observes the
same change, so the fleet would carry it once per worker, and someone would
then have to deduplicate, or be elected to publish alone. That is a relay and
a leader for information the table already holds.

## The shape now

A **directory-watcher table** follows the lanes: one slot per lane. A slot is a
wake word, a reactor flag and a pid — nothing else. A watcher serves no name,
takes no frames and never appears in the directory, so it is not a connection
and nobody sees it as one.

- **Taking a slot.** `LinkSegment::watch()` takes the first slot whose kernel
  hold it can win (`orbit_core::shm::try_hold_watcher`, an exclusive `flock` on
  a lock file of its own, `<segment>.watch<n>.lock`). A slot whose holder died
  is free because the kernel released the hold — the same rule that tells a
  dead lane from a live one, with no timeout and no pid check.
- **Being woken.** Every claim and release already ends in
  `announce_directory`, which now also bumps every watcher slot's word and
  rings each slot that is armed. The bell is unchanged: a one-byte datagram to
  a named socket, so a Tokio task sleeps on a descriptor without a reader
  thread. Watchers have their own addresses (`…w<n>`) because a named socket
  has exactly one binder.
- **Waiting.** `DirectoryWatcher::next()` reports the generation the directory
  moved to. It checks the generation, arms, checks again, and only then awaits
  the socket — the same order `InboxReceiver::next()` uses — so a change that
  lands between the look and the wait is not lost.

## Why this is the cell pattern

`orbit-cell` already works this way: a writer bumps the cell's word and wakes
whoever is parked; a waiter announces itself and parks. The waiter owns
nothing, and the writer never learns who listens. Nobody passes the news on,
so nobody has to be chosen to.

The directory is the cell here. A claim or release is the write. Lane holders
and watchers are the listeners.

The one difference is why a slot exists at all. A futex wakes every thread
parked on one word, so a cell needs only a count of waiters. A bell is a
datagram to one named socket, so each Tokio listener needs an address of its
own. The slot is that address; the rule is still "tell whoever is listening".

## Only whoever is listening at that moment

A wake goes only to a listener that is armed at that moment; ringing clears
the flag, so a burst of changes rings once. A listener that was not waiting
misses nothing: on its next look it compares the generation, sees it moved and
does not park.

That is the logical gain. **The generation is the truth; the wake is a hint.**
Correctness never depends on delivering a wake:

- a lost or coalesced wake makes a listener later, never wrong, because every
  look diffs the current table;
- there is no queue, no delivery guarantee and no replay, because nothing is
  carried — an observer derives what changed from state;
- a process that starts later reads the table as it is; it does not need the
  history of how it got there.

In a message model the same signal would be a record in a ring, and a lost
record would be a wrong state; order, duplicates, acknowledgement and an
elected publisher would follow from that. Here none of them arise.

## The intended use

A process that cares about connects and disconnects derives them itself
from the directory and delivers them locally: workers through the lane they
already read, a master through a watcher slot. Nothing is published to the
fleet for this, so nothing has to be deduplicated, and a connect or disconnect
is seen once per observing process.

## Layout and compatibility

- Inbox layout version `5`. A table of an earlier version is refused at
  attach, as every layout change is.
- The segment grows by one 64-byte slot per lane. Watchers sit **after** the
  lanes, so every lane's offset and alignment are what they were; the frame
  path (`write`, `read`, a lane's `wake`) does not touch a watcher. The extra
  work is one atomic add per slot on a claim or release, which traffic never
  causes.
- Because the segment's size changes, a deployment moves the inbox to a new
  kind rather than reopening the old one under a bigger shape: a process of
  the new build then boots beside processes of the old build instead of being
  refused by their segment. The old kind is retired and cleared once nothing
  holds it.
- `companion_lock_files` includes the watcher lock files, so whatever removes
  a segment removes them with it.
