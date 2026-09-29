#![cfg(unix)]

use std::ffi::CString;
use std::io::ErrorKind;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use orbit_core::ring::shm::{ShmRing, ShmRingView};
use orbit_core::shm::{
    ShmAccessPolicy, ShmRegion, ShmValidation, ring_segment_name, ring_segment_name_for_uid
};
use orbit_core::{Fleet, FleetObserver, NodeId, OrbitTyped, RingSpec};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fleet_name() -> String {
    format!("ap{:x}{:x}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}
fn uid() -> u32 {
    unsafe { libc::geteuid() }
}
fn gid() -> u32 {
    unsafe { libc::getegid() }
}

struct Cleanup(CString);
impl Cleanup {
    fn new(name: &str) -> Self {
        Self(CString::new(name).unwrap())
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        // Only the unique object owned by this test, never a fleet-wide cleanup.
        unsafe {
            libc::shm_unlink(self.0.as_ptr());
        }
    }
}

fn open(name: &str) -> OwnedFd {
    let name = CString::new(name).unwrap();
    let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDONLY, 0) };
    assert!(fd >= 0, "{}", std::io::Error::last_os_error());
    unsafe { OwnedFd::from_raw_fd(fd) }
}
fn stat(fd: &OwnedFd) -> libc::stat {
    let mut value = std::mem::MaybeUninit::uninit();
    assert_eq!(unsafe { libc::fstat(fd.as_raw_fd(), value.as_mut_ptr()) }, 0);
    unsafe { value.assume_init() }
}
fn raw_create(
    name: &str,
    mode: u32
) -> OwnedFd {
    let name = CString::new(name).unwrap();
    let fd =
        unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_EXCL | libc::O_RDWR, mode) };
    assert!(fd >= 0, "{}", std::io::Error::last_os_error());
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    assert_eq!(unsafe { libc::ftruncate(fd.as_raw_fd(), 8192) }, 0);
    fd
}
fn denied<T>(result: std::io::Result<T>) {
    match result {
        Err(error) => assert_eq!(error.kind(), ErrorKind::PermissionDenied, "{error}"),
        Ok(_) => panic!("unexpected access granted")
    }
}

// A subprocess owns umask changes; no process-wide mask races with parallel tests.
#[test]
fn access_policies_in_isolated_process() {
    const CHILD: &str = "ORBIT_ACCESS_POLICY_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "access_policies_in_isolated_process", "--nocapture"])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    unsafe {
        libc::umask(0);
    }
    defaults_and_modes();
    rejects_existing_without_mutation();
    owner_and_gid_checks();
    rings_and_fleet_configuration();
    narrowed_permissions_are_accepted();
    invalid_group_does_not_leave_an_object();
    #[cfg(target_os = "macos")]
    macos_rejects_creation_for_another_group();
    #[cfg(not(target_os = "macos"))]
    unix_sets_group_before_exposing_new_object();
}

fn defaults_and_modes() {
    assert_eq!(ShmAccessPolicy::default(), ShmAccessPolicy::OwnerOnly);
    for policy in [
        ShmAccessPolicy::OwnerOnly,
        ShmAccessPolicy::GroupRead { gid: gid() },
        ShmAccessPolicy::GroupReadWrite { gid: gid() }
    ] {
        let name = ring_segment_name(&fleet_name(), 250);
        let _cleanup = Cleanup::new(&name);
        let region = ShmRegion::open_or_create_with_policy(&name, 8192, policy).unwrap();
        assert!(region.created());
        let fd = open(&name);
        let metadata = stat(&fd);
        assert_eq!(u64::from(metadata.st_mode) & 0o7777, u64::from(policy.mode()));
        assert_eq!(metadata.st_uid, uid());
        assert_eq!(metadata.st_gid, gid());
        let peer = ShmRegion::open_or_create_with_policy(&name, 8192, policy).unwrap();
        assert!(!peer.created());
        assert!(matches!(
            ShmRegion::validate_existing_with_policy(&name, 8192, uid(), policy),
            Ok(ShmValidation::Valid { .. })
        ));
        if policy == ShmAccessPolicy::OwnerOnly {
            assert!(ShmRegion::open_or_create(&name, 8192).is_ok());
        } else {
            denied(ShmRegion::open_or_create(&name, 8192));
        }
    }
}

fn rejects_existing_without_mutation() {
    for mode in [0o644, 0o666, 0o700] {
        let fleet = fleet_name();
        let name = ring_segment_name(&fleet, 250);
        let _cleanup = Cleanup::new(&name);
        let fd = raw_create(&name, mode);
        let before = stat(&fd);
        denied(ShmRegion::open_or_create(&name, 1024));
        denied(ShmRegion::open_or_create_locked(&name, 1024));
        denied(ShmRegion::validate_existing(&name, 1024));
        // Permission checks run before looking at an untrusted ring header.
        denied(ShmRingView::attach_existing(&fleet, 250));
        denied(FleetObserver::attach_existing(&fleet).unwrap().ring(250));
        let after = stat(&open(&name));
        assert_eq!(before.st_mode, after.st_mode);
        assert_eq!(before.st_uid, after.st_uid);
        assert_eq!(before.st_gid, after.st_gid);
        assert_eq!(before.st_size, after.st_size);
    }
    let name = ring_segment_name(&fleet_name(), 250);
    let _cleanup = Cleanup::new(&name);
    let fd = raw_create(&name, 0o660);
    denied(ShmRegion::open_or_create_with_policy(
        &name,
        1024,
        ShmAccessPolicy::GroupRead { gid: gid() }
    ));
    assert_eq!(u64::from(stat(&fd).st_mode) & 0o777, 0o660);
}

fn owner_and_gid_checks() {
    let name = ring_segment_name(&fleet_name(), 250);
    let _cleanup = Cleanup::new(&name);
    let _region = ShmRegion::open_or_create(&name, 8192).unwrap();
    denied(ShmRegion::validate_existing_with_policy(
        &name,
        1,
        uid() + 1,
        ShmAccessPolicy::OwnerOnly
    ));
    let wrong_group = ShmAccessPolicy::GroupRead { gid: gid() + 1 };
    denied(ShmRegion::open_or_create_with_policy(&name, 8192, wrong_group));
    denied(ShmRegion::validate_existing_with_policy(&name, 1, uid(), wrong_group));

    let fleet = fleet_name();
    let fake_uid = uid() + 1;
    let fake_name = ring_segment_name_for_uid(&fleet, 250, fake_uid);
    let _cleanup_fake = Cleanup::new(&fake_name);
    let _fd = raw_create(&fake_name, 0o600);
    denied(ShmRingView::attach_existing_for_uid(&fleet, 250, fake_uid));
}

#[derive(Clone)]
struct Shared;
impl OrbitTyped for Shared {
    const KIND: u8 = 250;
    const RING_SPEC: RingSpec = RingSpec::per_node(4, 16);
}
#[derive(Clone)]
struct Private;
impl OrbitTyped for Private {
    const KIND: u8 = 251;
    const RING_SPEC: RingSpec = RingSpec::new(4, 16);
}
fn rings_and_fleet_configuration() {
    let name = fleet_name();
    let _shared_cleanup = Cleanup::new(&ring_segment_name(&name, Shared::KIND));
    let _private_cleanup = Cleanup::new(&ring_segment_name(&name, Private::KIND));
    let policy = ShmAccessPolicy::GroupRead { gid: gid() };
    let fleet =
        Fleet::join_shm_as_with_policies(&name, 2, NodeId::ZERO, [(Shared::KIND, policy)]).unwrap();
    let shared = fleet.shm_ring::<Shared>().unwrap();
    let private = fleet.shm_ring::<Private>().unwrap();
    assert_eq!(shared.spec(), Shared::RING_SPEC);
    assert!(std::sync::Arc::ptr_eq(&shared, &fleet.shm_ring::<Shared>().unwrap()));
    let id = shared.write(NodeId::ZERO, 0, 1, Bytes::from_static(b"hello")).unwrap();
    let observer = FleetObserver::attach_existing(&name).unwrap();
    denied(observer.ring(Shared::KIND));
    let view = observer.ring_with_policy(Shared::KIND, policy).unwrap();
    assert!(observer.typed_ring_with_policy::<Shared>(policy).is_ok());
    assert_eq!(view.metadata().spec, Shared::RING_SPEC);
    assert_eq!(view.lane(0).unwrap().read_head().unwrap().id, id);
    assert!(observer.ring(Private::KIND).is_ok());
    assert_eq!(
        u64::from(stat(&open(&ring_segment_name(&name, Private::KIND))).st_mode) & 0o777,
        0o600
    );
    let peer = ShmRing::open_or_create_for_fleet_with_policy(
        &name,
        Shared::KIND,
        Shared::RING_SPEC,
        2,
        policy
    )
    .unwrap();
    assert_eq!(peer.read(id).unwrap().payload.as_ref(), b"hello");
    denied(ShmRing::open_or_create_for_fleet(&name, Shared::KIND, Shared::RING_SPEC, 2));
    shared.unlink().unwrap();
    private.unlink().unwrap();
}

fn narrowed_permissions_are_accepted() {
    let name = ring_segment_name(&fleet_name(), 250);
    let _cleanup = Cleanup::new(&name);
    let _fd = raw_create(&name, 0o400);
    assert!(ShmRegion::validate_existing(&name, 1).is_ok());
    // No implicit process-wide umask change inside the library.
    unsafe {
        libc::umask(0o077);
    }
    let second = ring_segment_name(&fleet_name(), 250);
    let _second_cleanup = Cleanup::new(&second);
    let _region = ShmRegion::open_or_create(&second, 8192).unwrap();
    assert_eq!(u64::from(stat(&open(&second)).st_mode) & 0o777, 0o600);
    let third = ring_segment_name(&fleet_name(), 250);
    let _third_cleanup = Cleanup::new(&third);
    let _group = ShmRegion::open_or_create_with_policy(
        &third,
        8192,
        ShmAccessPolicy::GroupRead { gid: gid() }
    )
    .unwrap();
    #[cfg(target_os = "macos")]
    assert_eq!(u64::from(stat(&open(&third)).st_mode) & 0o777 & !0o640, 0);
    #[cfg(not(target_os = "macos"))]
    assert_eq!(u64::from(stat(&open(&third)).st_mode) & 0o777, 0o640);
    assert_eq!(unsafe { libc::umask(0) }, 0o077);
}

#[cfg(target_os = "macos")]
fn macos_rejects_creation_for_another_group() {
    let name = ring_segment_name(&fleet_name(), 250);
    let _cleanup = Cleanup::new(&name);
    let result = ShmRegion::open_or_create_with_policy(
        &name,
        8192,
        ShmAccessPolicy::GroupRead { gid: gid() + 1 }
    );
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::InvalidInput));
    assert_eq!(ShmRegion::validate_existing(&name, 1).unwrap(), ShmValidation::Missing);
}

fn invalid_group_does_not_leave_an_object() {
    let name = ring_segment_name(&fleet_name(), 250);
    let _cleanup = Cleanup::new(&name);
    let result = ShmRegion::open_or_create_with_policy(
        &name,
        8192,
        ShmAccessPolicy::GroupRead { gid: u32::MAX }
    );
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::InvalidInput));
    assert_eq!(ShmRegion::validate_existing(&name, 1).unwrap(), ShmValidation::Missing);
}

#[cfg(not(target_os = "macos"))]
fn unix_sets_group_before_exposing_new_object() {
    // Pick a permitted supplementary gid, if available, without changing process credentials.
    let groups = nix::unistd::getgroups().unwrap();
    let selected = groups.into_iter().map(|g| g.as_raw()).find(|g| *g != gid()).unwrap_or(gid());
    let name = ring_segment_name(&fleet_name(), 250);
    let _cleanup = Cleanup::new(&name);
    let policy = ShmAccessPolicy::GroupRead { gid: selected };
    let _region = ShmRegion::open_or_create_with_policy(&name, 8192, policy).unwrap();
    let metadata = stat(&open(&name));
    assert_eq!(metadata.st_gid, selected);
    assert_eq!(metadata.st_mode & 0o777, 0o640);
}
