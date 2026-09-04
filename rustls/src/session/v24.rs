//! rustls 0.24 server-session adapter.

use rustls_0_24::server::{ServerSessionKey, StoresServerSessions};

use super::OrbitSessionStorage;

impl StoresServerSessions for OrbitSessionStorage {
    fn put(&self, key: ServerSessionKey<'_>, value: Vec<u8>) -> bool {
        self.put_bytes(key.as_ref(), &value)
    }

    fn get(&self, key: ServerSessionKey<'_>) -> Option<Vec<u8>> {
        self.get_bytes(key.as_ref())
    }

    fn take(&self, key: ServerSessionKey<'_>) -> Option<Vec<u8>> {
        self.take_bytes(key.as_ref())
    }

    fn can_cache(&self) -> bool {
        true
    }
}
