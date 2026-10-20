use crate::Error;

/// Opening-time identity for the kernel boot and monotonic clock namespace.
/// No wall-clock timestamp or process-local Instant acts as shared time.
pub(super) fn clock_domain() -> Result<[u8; 32], Error> {
    #[cfg(target_os = "macos")]
    {
        let mut bytes = [0_u8; 128];
        let mut length = bytes.len();
        // SAFETY: a fixed writable buffer and its capacity are supplied.
        let result = unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                bytes.as_mut_ptr().cast(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        };
        if result != 0 || length == 0 || length > bytes.len() {
            return Err(Error::ClockUnavailable);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"orbit-quota/macos/CLOCK_MONOTONIC/v1");
        hash.update(&bytes[..length]);
        Ok(*hash.finalize().as_bytes())
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let boot = std::fs::read("/proc/sys/kernel/random/boot_id")
            .map_err(|_| Error::ClockUnavailable)?;
        let namespace = std::fs::metadata("/proc/self/ns/time")
            .map_err(|_| Error::ClockUnavailable)?;
        if boot.is_empty() {
            return Err(Error::ClockUnavailable);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"orbit-quota/linux/CLOCK_MONOTONIC/v1");
        hash.update(&boot);
        hash.update(&namespace.dev().to_le_bytes());
        hash.update(&namespace.ino().to_le_bytes());
        Ok(*hash.finalize().as_bytes())
    }
    #[cfg(target_os = "freebsd")]
    {
        let mut boot = [0_u8; 16];
        let mut length = boot.len();
        // SAFETY: the output buffer has the exact boot UUID capacity and the
        // kernel receives its length. No data is used after a failed read.
        let result = unsafe {
            libc::sysctlbyname(
                c"kern.boot_id".as_ptr(),
                boot.as_mut_ptr().cast(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        };
        if result != 0 || length != boot.len() {
            return Err(Error::ClockUnavailable);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"orbit-quota/freebsd/CLOCK_MONOTONIC/v1");
        hash.update(&boot);
        Ok(*hash.finalize().as_bytes())
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "linux",
        target_os = "freebsd"
    )))]
    {
        // A backend without a verified boot/clock-domain identity fails closed.
        Err(Error::ClockUnavailable)
    }
}
