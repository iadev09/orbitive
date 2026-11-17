//! `std::sync::atomic` between the processes of a fleet.
//!
//! [`Orbitals`] is one fleet-shared table of 64-bit values in Orbit shared
//! memory. [`Orbitals::allocate`] declares a value and returns an
//! [`Orbital`], a handle with the operations of an atomic. Its
//! [`OrbitalId`] is the only thing that travels: any process of the fleet
//! that [`Orbitals::open`]s the id holds the same value, and can
//! [`Orbital::wait`] for it to change.
//!
//! A value with a fixed identity is declared by name instead:
//! [`Orbitals::declare`] returns the same value to every process that
//! declares the name, and [`orbital!`] gives such a value a type of its own
//! that implements [`Declared`].
//!
//! A value lives as long as its table's shared memory. Nothing releases a
//! single value; the table's owner clears all of them with
//! [`Orbitals::reset_all`] when it boots with nothing else attached, except
//! those declared [`Lifetime::Kept`].

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::ring_segment_name;

mod declared;
mod error;
mod id;
mod names;
mod orbital;
mod table;
mod wake;

pub use declared::Declared;
pub use error::{Error, Result};
pub use id::{Kind, OrbitalId, Primitive};
#[doc(hidden)]
pub use names::assert_declared_name;
pub use names::{ATOMIC_NAME_MAX, ATOMIC_NAMES_KIND, Lifetime};
pub use orbital::Orbital;
pub use table::{ATOMIC_CAPACITY, ATOMIC_KIND};

use table::Table;

/// Whether this build can park on a shared word. A table refuses to open
/// where it cannot: a sleep loop wearing the shape of a wait is worse than
/// a clear no.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
fn waits_supported() -> bool {
    orbit_core::sync::supported()
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
fn waits_supported() -> bool {
    false
}

/// Park until `word` no longer holds `expected`, through the platform's
/// shared address wait. There is no polling fallback: a table refuses to
/// open where the platform cannot wait, so by here it can.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub(crate) fn wait_on(
    word: &AtomicU32,
    expected: u32,
) -> Result<()> {
    orbit_core::sync::wait_word(word, expected).map_err(Error::Io)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
pub(crate) fn wait_on(
    _word: &AtomicU32,
    _expected: u32,
) -> Result<()> {
    Err(unsupported())
}

/// Wake everyone parked on `word`; nothing to do where nobody can park.
pub(crate) fn wake_on(word: &AtomicU32) {
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    let _ = orbit_core::sync::wake_word(word);
    #[cfg(not(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    )))]
    let _ = word;
}

fn unsupported() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "orbit-atomic needs a platform that can wait on a shared word: Linux, FreeBSD, or macOS 14.4 or later",
    ))
}

/// The fleet's table of shared values. Cheap to clone.
#[derive(Clone)]
pub struct Orbitals {
    table: Arc<Table>,
}

impl Orbitals {
    /// Attach to the fleet's table, creating it when this is the first
    /// process to ask. An in-memory fleet gets a table on the heap, shared
    /// by everything that holds the same fleet.
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        if !waits_supported() {
            return Err(unsupported());
        }
        let table = if fleet.is_shm() {
            #[cfg(unix)]
            {
                table::shm_table(
                    &ring_segment_name(fleet.name(), ATOMIC_KIND),
                    &ring_segment_name(fleet.name(), ATOMIC_NAMES_KIND),
                )?
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            table::memory_table(&fleet)
        };
        Ok(Self { table })
    }

    /// Declare a value holding `initial`. The id inside the handle is what
    /// other processes open.
    ///
    /// The slot is not given back until the table is reset, so the
    /// capacity counts declarations, not live values: declare a value once
    /// and keep it, as a `static`, rather than one per request.
    pub fn allocate<T: Primitive>(
        &self,
        initial: T,
    ) -> Result<Orbital<T>> {
        let (id, slot) = self.table.allocate(T::KIND, initial.into_word())?;
        Ok(Orbital::new(slot, id, Arc::clone(&self.table)))
    }

    /// The value called `name`: the first declaration in the table's epoch
    /// allocates a slot holding `initial`, and every later one, in any
    /// process, gets a handle to that slot and ignores `initial`.
    ///
    /// The name is forgotten at [`Self::reset_all`]; [`Self::declare_kept`]
    /// declares one that survives it. Declaring a name with another type
    /// is [`Error::KindMismatch`], with the other lifetime
    /// [`Error::LifetimeMismatch`]. The name is looked up only here: hold
    /// the handle, not the name.
    pub fn declare<T: Primitive>(
        &self,
        name: &str,
        initial: T,
    ) -> Result<Orbital<T>> {
        self.declare_with(name, initial, Lifetime::Reset)
    }

    /// [`Self::declare`], for a value that keeps its name and its value
    /// across [`Self::reset_all`].
    pub fn declare_kept<T: Primitive>(
        &self,
        name: &str,
        initial: T,
    ) -> Result<Orbital<T>> {
        self.declare_with(name, initial, Lifetime::Kept)
    }

    fn declare_with<T: Primitive>(
        &self,
        name: &str,
        initial: T,
        lifetime: Lifetime,
    ) -> Result<Orbital<T>> {
        let resolved =
            self.table.declare(name, T::KIND, initial.into_word(), lifetime)?;
        let id = OrbitalId::from_bytes(resolved.id.to_be_bytes())?;
        if id.kind() != T::KIND {
            return Err(Error::KindMismatch { id, expected: T::KIND });
        }
        if resolved.lifetime != Some(lifetime) {
            return Err(Error::LifetimeMismatch { id, expected: lifetime });
        }
        self.open(id)
    }

    /// A handle to the value `id` names. An id of another type is
    /// [`Error::KindMismatch`]; an id this table did not mint, or minted
    /// before its last reset, is [`Error::Stale`].
    pub fn open<T: Primitive>(
        &self,
        id: OrbitalId,
    ) -> Result<Orbital<T>> {
        let slot = self.table.open(id, T::KIND)?;
        Ok(Orbital::new(slot, id, Arc::clone(&self.table)))
    }

    /// Empty the table and start a new epoch, after which every earlier id
    /// is stale. Names are forgotten, except those declared with
    /// [`Self::declare_kept`]: they keep their values and are found again
    /// by declaring them, under new ids.
    ///
    /// For the table's owner, at a boot where no other process is attached
    /// and no handle is in use. A handle kept across a reset still points
    /// at its slot, which the next allocation may hand to someone else.
    pub fn reset_all(&self) -> Result<()> {
        self.table.reset(true)
    }

    /// Remove the table's SHM object. Existing mappings remain valid until
    /// their processes drop them.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.table.unlink()
    }
}
