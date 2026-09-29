//! rustls 0.24 fleet-shared client-session adapter.

use rustls_0_24::client::{ClientSessionKey, ClientSessionStore, Tls12Session, Tls13Session};
use rustls_0_24::crypto::kx::NamedGroup;
use rustls_0_24::pki_types::ServerName;

use super::OrbitClientSessionStorage;
use super::store::CLIENT_TLS13_TICKETS_PER_SERVER;

const KX_HINT: u8 = 1;
const TLS12_SESSION: u8 = 2;
const TLS13_TICKET: u8 = 3;
const DNS_NAME: u8 = 1;
const IP_ADDRESS: u8 = 2;

impl ClientSessionStore for OrbitClientSessionStorage {
    fn set_kx_hint(
        &self,
        key: ClientSessionKey<'static>,
        group: NamedGroup
    ) {
        let Some((domain, server)) = storage_key(&key, KX_HINT) else {
            return;
        };
        let value = u16::from(group).to_be_bytes();
        let _ = self.primitive.put(&domain, &server, &value, self.ttl());
    }

    fn kx_hint(
        &self,
        key: &ClientSessionKey<'_>
    ) -> Option<NamedGroup> {
        let (domain, server) = storage_key(key, KX_HINT)?;
        let value = self.primitive.get(&domain, &server).ok()??;
        let value: [u8; 2] = value.try_into().ok()?;
        Some(NamedGroup::from(u16::from_be_bytes(value)))
    }

    fn set_tls12_session(
        &self,
        key: ClientSessionKey<'static>,
        value: Tls12Session
    ) {
        let Some((domain, server)) = storage_key(&key, TLS12_SESSION) else {
            return;
        };
        let mut encoded = Vec::new();
        value.encode(&mut encoded);
        let _ = self.primitive.put(&domain, &server, &encoded, self.ttl());
    }

    fn tls12_session(
        &self,
        key: &ClientSessionKey<'_>
    ) -> Option<Tls12Session> {
        let (domain, server) = storage_key(key, TLS12_SESSION)?;
        let encoded = self.primitive.get(&domain, &server).ok()??;
        match Tls12Session::from_slice(&encoded, &self.provider) {
            Ok(value) => Some(value),
            Err(error) => {
                let _ = self.primitive.take(&domain, &server);
                tracing::trace!(
                    target: "orbit_rustls::client_session_cache",
                    operation = "decode_tls12",
                    %error,
                    "discarding incompatible rustls client session"
                );
                None
            }
        }
    }

    fn remove_tls12_session(
        &self,
        key: &ClientSessionKey<'static>
    ) {
        let Some((domain, server)) = storage_key(key, TLS12_SESSION) else {
            return;
        };
        let _ = self.primitive.take(&domain, &server);
    }

    fn insert_tls13_ticket(
        &self,
        key: ClientSessionKey<'static>,
        value: Tls13Session
    ) {
        let Some((domain, server)) = storage_key(&key, TLS13_TICKET) else {
            return;
        };
        let mut encoded = Vec::new();
        value.encode(&mut encoded);
        let _ = self.primitive.push(
            &domain,
            &server,
            &encoded,
            self.ttl(),
            CLIENT_TLS13_TICKETS_PER_SERVER
        );
    }

    fn take_tls13_ticket(
        &self,
        key: &ClientSessionKey<'static>
    ) -> Option<Tls13Session> {
        let (domain, server) = storage_key(key, TLS13_TICKET)?;
        for _ in 0..CLIENT_TLS13_TICKETS_PER_SERVER {
            let encoded = match self.primitive.take(&domain, &server) {
                Ok(Some(encoded)) => encoded,
                Ok(None) | Err(_) => return None
            };
            match Tls13Session::from_slice(&encoded, &self.provider) {
                Ok(value) => return Some(value),
                Err(error) => {
                    tracing::trace!(
                        target: "orbit_rustls::client_session_cache",
                        operation = "decode_tls13",
                        %error,
                        "discarding incompatible rustls client ticket"
                    );
                }
            }
        }
        None
    }
}

fn storage_key(
    key: &ClientSessionKey<'_>,
    record: u8
) -> Option<([u8; 33], Vec<u8>)> {
    let mut domain = [0_u8; 33];
    domain[0] = record;
    domain[1..].copy_from_slice(&key.config_hash);

    let mut server = Vec::new();
    server.push(match &key.server_name {
        ServerName::DnsName(_) => DNS_NAME,
        ServerName::IpAddress(_) => IP_ADDRESS,
        _ => return None
    });
    server.extend_from_slice(key.server_name.to_str().as_bytes());
    Some((domain, server))
}
