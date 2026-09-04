//! rustls 0.23 server-session adapter.

use rustls_0_23::server::StoresServerSessions;

use super::OrbitSessionStorage;

impl StoresServerSessions for OrbitSessionStorage {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        self.put_bytes(&key, &value)
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_bytes(key)
    }

    fn take(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.take_bytes(key)
    }

    fn can_cache(&self) -> bool {
        true
    }
}
