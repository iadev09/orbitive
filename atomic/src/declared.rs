//! Values with a fixed name: [`Declared`] and the [`orbital!`] macro that
//! implements it.
//!
//! [`orbital!`]: crate::orbital

use std::fmt;
use std::ops::Deref;

use crate::{Lifetime, Orbital, Orbitals, Primitive, Result};

/// A value with a fixed name that every process of the fleet reaches by
/// declaring it. Implemented by [`orbital!`](crate::orbital); a host that
/// registers or injects declared values works over this trait.
///
/// The handle derefs to [`Orbital`], so the value operations are the
/// handle's own; the name is looked up only in [`Self::open`].
pub trait Declared:
    Clone
    + fmt::Debug
    + Send
    + Sync
    + 'static
    + Deref<Target = Orbital<<Self as Declared>::Value>>
{
    /// What the value holds.
    type Value: Primitive;
    /// The name every process declares it by.
    const NAME: &'static str;
    /// What the first declaration in a table's epoch installs. Later
    /// declarations, in any process, find the value and ignore this.
    const INITIAL: Self::Value;
    /// Whether the value survives [`Orbitals::reset_all`].
    const LIFETIME: Lifetime;

    /// Declare the value in `orbitals`: the first caller installs it, every
    /// later one gets a handle to the same slot.
    fn open(orbitals: &Orbitals) -> Result<Self>;
}

/// Declare named values: for each, a handle type that derefs to
/// [`Orbital`] and implements [`Declared`].
///
/// ```
/// # use std::sync::Arc;
/// # use orbit_core::Fleet;
/// use orbit_atomic::{Orbitals, orbital};
///
/// orbital! {
///     /// The fleet's hit counter; starts again from 0 at a reset.
///     pub Hits: i64 = 0, "hits"
///     /// Survives `reset_all` with its value.
///     pub Visitors: u64 = 0, "visitors", kept
/// }
///
/// # let orbitals = Orbitals::new(Arc::new(Fleet::join("example", 1)?))?;
/// let hits = Hits::open(&orbitals)?; // any process; the first one installs
/// hits.fetch_add(1);                  // the Orbital itself: no name, no lookup
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Each declaration is `visibility Type: type = initial, "name"`, with
/// `, kept` after the name for a value that survives a reset. The initial
/// value is a constant expression. A name that is empty or longer than
/// [`ATOMIC_NAME_MAX`](crate::ATOMIC_NAME_MAX) does not compile.
#[macro_export]
macro_rules! orbital {
    () => {};
    (
        @declare [$(#[$attr:meta])*] [$vis:vis]
        $type:ident, $value:ty, $initial:expr, $name:literal, $lifetime:ident
    ) => {
        $(#[$attr])*
        #[derive(Clone, Debug)]
        $vis struct $type($crate::Orbital<$value>);

        const _: () = $crate::assert_declared_name($name);

        impl $type {
            /// Declare this value: the first caller in the table's epoch
            /// installs it, every later one gets the same slot.
            #[allow(dead_code)]
            $vis fn open(orbitals: &$crate::Orbitals) -> $crate::Result<Self> {
                <Self as $crate::Declared>::open(orbitals)
            }
        }

        impl $crate::Declared for $type {
            type Value = $value;
            const NAME: &'static str = $name;
            const INITIAL: $value = $initial;
            const LIFETIME: $crate::Lifetime = $crate::Lifetime::$lifetime;

            fn open(orbitals: &$crate::Orbitals) -> $crate::Result<Self> {
                match <Self as $crate::Declared>::LIFETIME {
                    $crate::Lifetime::Reset => {
                        orbitals.declare(Self::NAME, Self::INITIAL)
                    }
                    $crate::Lifetime::Kept => {
                        orbitals.declare_kept(Self::NAME, Self::INITIAL)
                    }
                }
                .map(Self)
            }
        }

        impl ::core::ops::Deref for $type {
            type Target = $crate::Orbital<$value>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
    (
        $(#[$attr:meta])*
        $vis:vis $type:ident : $value:ty = $initial:expr, $name:literal, kept
        $($rest:tt)*
    ) => {
        $crate::orbital!(
            @declare [$(#[$attr])*] [$vis] $type, $value, $initial, $name, Kept
        );
        $crate::orbital!($($rest)*);
    };
    (
        $(#[$attr:meta])*
        $vis:vis $type:ident : $value:ty = $initial:expr, $name:literal
        $($rest:tt)*
    ) => {
        $crate::orbital!(
            @declare [$(#[$attr])*] [$vis] $type, $value, $initial, $name, Reset
        );
        $crate::orbital!($($rest)*);
    };
}
