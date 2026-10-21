//! Explicit group access to dedicated IPC resources, never a fleet-wide policy.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use super::{LaneHold, ShmAccessPolicy, ShmRegion, ShmRegionLock, WatcherHold};

/// Access to one deployment-owned IPC directory and its named SHM objects.
///
/// All group writers are trusted peers, not isolated principals. The owner
/// prepares lock inodes; other users only attach. The parent directory must
/// be provisioned by the operator and protected from untrusted replacement.
/// No credentials, umask, existing permissions or unrelated resources change.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GroupAccess {
    owner: u32,
    gid: u32,
    directory: PathBuf,
}

impl GroupAccess {
    pub fn new(
        owner: u32,
        gid: u32,
        directory: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let directory = directory.as_ref();
        if owner == !0 || gid == !0 || !directory.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "explicit owner, group and absolute IPC directory required",
            ));
        }
        let leaf = directory.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC directory must not be a filesystem root",
            )
        })?;
        let parent = directory.parent().unwrap().canonicalize()?;
        // Resolve only the trusted parent, never a preexisting final symlink.
        Ok(Self { owner, gid, directory: parent.join(leaf) })
    }

    pub fn owner_uid(&self) -> u32 {
        self.owner
    }
    pub fn gid(&self) -> u32 {
        self.gid
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn is_owner(&self) -> bool {
        (unsafe { libc::geteuid() }) == self.owner
    }

    /// Scope the SHM name to the same owner/group/directory as its kernel locks.
    /// This prevents two IPC directories from supplying different death locks
    /// for the same mapped inbox. The name also fits Darwin's 31-byte limit.
    pub fn segment_name(
        &self,
        fleet: &str,
        kind: u8,
    ) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(b"orbit/group-resource/v1\0");
        hash.update(&self.owner.to_le_bytes());
        hash.update(&self.gid.to_le_bytes());
        hash.update(&[kind]);
        let directory = self.directory.as_os_str().as_encoded_bytes();
        hash.update(&(directory.len() as u64).to_le_bytes());
        hash.update(directory);
        hash.update(fleet.as_bytes());
        format!("/orbit-{}", &hash.finalize().to_hex()[..24])
    }

    /// Prepare once as owner; attaching peers never create directories.
    pub fn prepare(&self) -> io::Result<()> {
        self.open_directory(&self.directory, 0o750)?;
        self.open_directory(&self.directory.join("bells"), 0o2770)?;
        Ok(())
    }

    fn open_directory(
        &self,
        path: &Path,
        mode: u32,
    ) -> io::Result<File> {
        let created = if self.is_owner() {
            match std::fs::DirBuilder::new().mode(0o700).create(path) {
                Ok(()) => true,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => false,
                Err(e) => return Err(e),
            }
        } else {
            false
        };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(
                libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
            .open(path)?;
        if created {
            self.configure(&file, mode)?;
        }
        let m = file.metadata()?;
        if !m.is_dir()
            || m.uid() != self.owner
            || m.gid() != self.gid
            || m.mode() & 0o7777 != mode
        {
            return Err(denied("IPC directory owner/group/mode mismatch"));
        }
        Ok(file)
    }

    fn configure(
        &self,
        file: &File,
        mode: u32,
    ) -> io::Result<()> {
        if unsafe { libc::fchown(file.as_raw_fd(), !0, self.gid) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn lock_file(
        &self,
        name: &str,
    ) -> io::Result<std::os::fd::OwnedFd> {
        use std::ffi::CString;
        use std::os::fd::{FromRawFd, OwnedFd};
        if name.is_empty() || name.contains('/') || name == "." || name == ".."
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid IPC lock name",
            ));
        }
        let directory = self.open_directory(&self.directory, 0o750)?;
        let name = CString::new(name)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut created = false;
        let flags = libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let raw = if self.is_owner() {
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    flags | libc::O_CREAT | libc::O_EXCL,
                    0o600 as libc::c_uint,
                )
            };
            if fd >= 0 {
                created = true;
                fd
            } else if io::Error::last_os_error().raw_os_error()
                == Some(libc::EEXIST)
            {
                unsafe {
                    libc::openat(directory.as_raw_fd(), name.as_ptr(), flags)
                }
            } else {
                return Err(io::Error::last_os_error());
            }
        } else {
            unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) }
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let file = File::from(unsafe { OwnedFd::from_raw_fd(raw) });
        if created {
            self.configure(&file, 0o660)?;
        }
        let m = file.metadata()?;
        if !m.is_file()
            || m.uid() != self.owner
            || m.gid() != self.gid
            || m.mode() & 0o7777 != 0o660
            || m.nlink() != 1
        {
            return Err(denied("IPC lock owner/group/mode/type mismatch"));
        }
        Ok(file.into())
    }

    /// Prepare bounded lane/watcher lock files before publishing the inbox.
    pub fn prepare_locks(
        &self,
        segment: &str,
        lanes: usize,
    ) -> io::Result<()> {
        self.prepare()?;
        for lane in 0..lanes {
            self.lock_file(&format!(
                "{}.lane{lane}.lock",
                segment.trim_start_matches('/')
            ))?;
            self.lock_file(&format!(
                "{}.watch{lane}.lock",
                segment.trim_start_matches('/')
            ))?;
        }
        Ok(())
    }

    pub fn open_region(
        &self,
        name: &str,
        size: usize,
    ) -> io::Result<(ShmRegion, ShmRegionLock)> {
        self.prepare()?;
        let fd =
            self.lock_file(&format!("{}.lock", name.trim_start_matches('/')))?;
        let lock = super::lock_fd_exclusive(fd)?;
        let policy = ShmAccessPolicy::GroupReadWrite { gid: self.gid };
        let region = if self.is_owner() {
            ShmRegion::open_or_create_with_policy(name, size, policy)?
        } else {
            ShmRegion::open_existing_read_write(name, size, self.owner, policy)?
        };
        // In particular Darwin cannot repair creation permissions afterwards.
        // Refuse a creation mask that would silently omit group write access.
        let result = self.verify_shared_mode(name);
        if let Err(error) = result {
            if region.created() {
                // Keep group lock inodes, including the initialization lock
                // held by this call. Never visit the private lock directory.
                let _ = unsafe { libc::shm_unlink(region.name.as_ptr()) };
            }
            return Err(error);
        }
        Ok((region, lock))
    }

    /// Remove only a group-shared name. Persistent lock inodes are not removed
    /// on restart and private per-UID companion directories are never visited.
    /// All participants must already be stopped, as with ordinary SHM unlink.
    pub fn unlink_region(
        &self,
        region: &ShmRegion,
    ) -> io::Result<()> {
        if !self.is_owner() {
            return Err(denied("only the group resource owner may unlink"));
        }
        self.verify_shared_mode(
            region
                .name
                .to_str()
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
        )?;
        if unsafe { libc::shm_unlink(region.name.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn verify_shared_mode(
        &self,
        name: &str,
    ) -> io::Result<()> {
        use std::os::fd::{FromRawFd, OwnedFd};
        let name = std::ffi::CString::new(name)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let raw = unsafe {
            libc::shm_open(name.as_ptr(), libc::O_RDONLY, 0 as libc::c_uint)
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let file = File::from(unsafe { OwnedFd::from_raw_fd(raw) });
        let m = file.metadata()?;
        if m.uid() != self.owner
            || m.gid() != self.gid
            || m.mode() & 0o7777 != 0o660
        {
            return Err(denied(
                "shared SHM requires exact owner/group and mode 0660; check the owner's creation mask",
            ));
        }
        Ok(())
    }

    pub fn try_hold_lane(
        &self,
        segment: &str,
        lane: usize,
    ) -> io::Result<Option<LaneHold>> {
        Ok(self
            .try_hold(&format!(
                "{}.lane{lane}.lock",
                segment.trim_start_matches('/')
            ))?
            .map(|lock_fd| LaneHold { lock_fd }))
    }

    pub fn try_hold_watcher(
        &self,
        segment: &str,
        slot: usize,
    ) -> io::Result<Option<WatcherHold>> {
        Ok(self
            .try_hold(&format!(
                "{}.watch{slot}.lock",
                segment.trim_start_matches('/')
            ))?
            .map(|lock_fd| WatcherHold { lock_fd }))
    }

    fn try_hold(
        &self,
        name: &str,
    ) -> io::Result<Option<std::os::fd::OwnedFd>> {
        let fd = self.lock_file(name)?;
        if unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }
            == 0
        {
            return Ok(Some(fd));
        }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock { Ok(None) } else { Err(e) }
    }

    /// The group-writable bell directory is separate from immutable lock names.
    /// A caller must own the corresponding lane before binding/removing a bell.
    pub fn bell_path(
        &self,
        segment: &str,
        suffix: &str,
    ) -> PathBuf {
        self.directory
            .join("bells")
            .join(format!("{}.{suffix}", segment.trim_start_matches('/')))
    }

    pub fn bind_bell(
        &self,
        path: &Path,
    ) -> io::Result<std::os::unix::net::UnixDatagram> {
        use std::os::unix::fs::FileTypeExt;
        let expected = self.directory.join("bells");
        if path.parent() != Some(expected.as_path()) {
            return Err(denied("bell outside IPC directory"));
        }
        self.open_directory(&expected, 0o2770)?;
        match std::fs::symlink_metadata(path) {
            Ok(m) if m.file_type().is_socket() && m.gid() == self.gid => {
                std::fs::remove_file(path)?
            }
            Ok(_) => {
                return Err(denied("refusing to replace non-socket IPC bell"));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let socket = std::os::unix::net::UnixDatagram::bind(path)?;
        // Setgid inheritance supplies the group. Never chmod/chown a pathname
        // in the writable bell directory: a peer could swap it for a symlink.
        let m = std::fs::symlink_metadata(path)?;
        if !m.file_type().is_socket()
            || m.gid() != self.gid
            || m.mode() & 0o020 == 0
        {
            return Err(denied(
                "IPC socket group/write permission mismatch; check creation mask",
            ));
        }
        socket.set_nonblocking(true)?;
        Ok(socket)
    }
}

fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
