use crate::Error;

/// Private clock injection keeps request-controlled time out of the public API.
pub(crate) trait Clock: Send + Sync {
    fn now(&self) -> Result<u128, Error>;
}

pub(crate) struct Monotonic;

impl Clock for Monotonic {
    fn now(&self) -> Result<u128, Error> {
        #[cfg(unix)]
        {
            let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
            // SAFETY: clock_gettime initializes the timespec on success. The
            // pointer is valid and its contents are read only after success.
            if unsafe {
                libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr())
            } != 0
            {
                return Err(Error::ClockUnavailable);
            }
            // SAFETY: the preceding successful call initialized this value.
            let time = unsafe { time.assume_init() };
            let seconds = u128::try_from(time.tv_sec)
                .map_err(|_| Error::ClockUnavailable)?;
            let nanos = u128::try_from(time.tv_nsec)
                .map_err(|_| Error::ClockUnavailable)?;
            if nanos >= 1_000_000_000 {
                return Err(Error::ClockUnavailable);
            }
            seconds
                .checked_mul(1_000_000_000)
                .and_then(|value| value.checked_add(nanos))
                .ok_or(Error::ArithmeticOverflow)
        }
        #[cfg(not(unix))]
        {
            Err(Error::ClockUnavailable)
        }
    }
}
