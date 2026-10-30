use crate::config::{ConfigStore, DeviceId, PairingKey, TrustedPeer, random_bytes};
use crate::crypto::ConnectionRole;
use anyhow::{Context, Result, bail};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const PAIRING_LIFETIME: Duration = Duration::from_secs(120);
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_ATTEMPTS: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinProgress {
    Discovering,
    Connecting,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredPeer {
    pub id: DeviceId,
    pub name: String,
    pub addresses: Vec<SocketAddr>,
    pub inviting: bool,
}

#[derive(Clone)]
pub(crate) struct PairAttempt {
    generation: DeviceId,
    pub code: String,
    expires: Instant,
}

struct Invitation {
    attempt: PairAttempt,
    attempts: u8,
}

pub(crate) struct PairRequest {
    pub attempt: PairAttempt,
    pub device: Option<String>,
    pub endpoint: Option<SocketAddr>,
    pub started: bool,
    discovery_deadline: Instant,
}

pub(crate) struct Pairing {
    pub store: ConfigStore,
    pub discovered: Vec<DiscoveredPeer>,
    invitation: Option<Invitation>,
    pub(crate) request: Option<PairRequest>,
    pub result: Option<String>,
    pub error: Option<String>,
}

pub(crate) type SharedPairing = Arc<Mutex<Pairing>>;

impl Pairing {
    pub fn new(store: ConfigStore) -> SharedPairing {
        Arc::new(Mutex::new(Self {
            store,
            discovered: Vec::new(),
            invitation: None,
            request: None,
            result: None,
            error: None,
        }))
    }

    pub fn invite(&mut self) -> Result<String> {
        self.cancel();
        self.start_invitation()
    }

    fn start_invitation(&mut self) -> Result<String> {
        // Rejection sampling includes leading zeros and avoids modulo bias.
        let number = loop {
            let number = u32::from_le_bytes(random_bytes()?);
            if number < u32::MAX - u32::MAX % 1_000_000 {
                break number % 1_000_000;
            }
        };
        let code = format!("{number:06}");
        let attempt = PairAttempt {
            generation: random_bytes()?,
            code: code.clone(),
            expires: Instant::now() + PAIRING_LIFETIME,
        };
        self.invitation = Some(Invitation {
            attempt,
            attempts: 0,
        });
        self.error = None;
        Ok(code)
    }

    pub fn is_inviting(&self) -> bool {
        // Renewal must not briefly withdraw the discovery advertisement.
        self.invitation.is_some()
    }

    pub fn invitation_code(&self) -> Option<&str> {
        self.invitation
            .as_ref()
            .filter(|i| i.attempt.expires > Instant::now())
            .map(|i| i.attempt.code.as_str())
    }

    pub fn seconds_left(&self) -> u64 {
        self.invitation.as_ref().map_or(0, |i| {
            i.attempt
                .expires
                .saturating_duration_since(Instant::now())
                .as_secs()
        })
    }

    pub fn join_progress(&self) -> Option<JoinProgress> {
        self.request.as_ref().map(|request| {
            if request.started || request.endpoint.is_some() {
                JoinProgress::Connecting
            } else {
                JoinProgress::Discovering
            }
        })
    }

    pub fn join(&mut self, code: &str, device: Option<String>) -> Result<()> {
        self.request_pairing(code, device, None)
    }

    pub fn join_at(&mut self, code: &str, endpoint: SocketAddr) -> Result<()> {
        crate::address::validate_pairing_address(endpoint)?;
        self.request_pairing(code, None, Some(endpoint))
    }

    fn request_pairing(
        &mut self,
        code: &str,
        device: Option<String>,
        endpoint: Option<SocketAddr>,
    ) -> Result<()> {
        let code = parse_code(code)?;
        let attempt = PairAttempt {
            generation: random_bytes()?,
            code,
            expires: Instant::now() + PAIRING_LIFETIME,
        };
        self.cancel();
        self.request = Some(PairRequest {
            attempt,
            device,
            endpoint,
            started: false,
            discovery_deadline: Instant::now() + DISCOVERY_TIMEOUT,
        });
        Ok(())
    }

    pub fn cancel(&mut self) {
        self.invitation = None;
        self.request = None;
        self.result = None;
        self.error = None;
    }

    pub fn key_for(&self, id: DeviceId) -> Result<PairingKey> {
        self.store
            .config
            .peers
            .iter()
            .find(|peer| peer.id == id)
            .map(|peer| peer.key)
            .context(crate::i18n::tr("pairing.not_paired"))
    }

    pub(crate) fn begin_incoming(&mut self) -> Result<PairAttempt> {
        self.expire();
        let invitation = self
            .invitation
            .as_mut()
            .context(crate::i18n::tr("pairing.invitation_expired"))?;
        if invitation.attempts >= MAX_ATTEMPTS {
            crate::i18n::localized!(bail, "pairing.too_many_attempts")
        }
        invitation.attempts += 1;
        Ok(invitation.attempt.clone())
    }

    // Only called after SPAKE2 and the encrypted handshake confirm the code.
    pub(crate) fn accept(
        &mut self,
        peer: TrustedPeer,
        attempt: &PairAttempt,
        role: ConnectionRole,
    ) -> Result<()> {
        let current = match role {
            ConnectionRole::Initiator => self.request.as_ref().map(|r| &r.attempt),
            ConnectionRole::Responder => self.invitation.as_ref().map(|i| &i.attempt),
        };
        if !current
            .is_some_and(|a| a.generation == attempt.generation && a.expires > Instant::now())
        {
            crate::i18n::localized!(bail, "pairing.attempt_expired")
        }
        self.store.trust(peer)?;
        match role {
            ConnectionRole::Initiator => self.request = None,
            ConnectionRole::Responder => self.invitation = None,
        }
        Ok(())
    }

    pub(crate) fn failed(&mut self, attempt: &PairAttempt, role: ConnectionRole) {
        if role == ConnectionRole::Initiator {
            if self
                .request
                .as_ref()
                .is_some_and(|r| r.attempt.generation == attempt.generation)
            {
                self.request = None;
                self.error = Some(crate::i18n::tr("pairing.connection_failed").into());
            }
        } else if self.invitation.as_ref().is_some_and(|i| {
            i.attempt.generation == attempt.generation && i.attempts >= MAX_ATTEMPTS
        }) {
            // Keep invitation mode usable after exhausting the guess budget.
            // Rotating here also means the UI never needs a manual regenerate
            // action for a recoverable pairing failure.
            if self.start_invitation().is_err() {
                self.invitation = None;
                self.error = Some(crate::i18n::tr("pairing.generate_code_failed").into());
            }
        }
    }

    pub fn expire(&mut self) {
        let now = Instant::now();
        if self
            .invitation
            .as_ref()
            .is_some_and(|i| i.attempt.expires <= now)
        {
            // Keep the user's invitation mode active and rotate the short-lived
            // code in place. The discovery advertisement remains available, so
            // another device can continue pairing without reopening the panel.
            if self.start_invitation().is_err() {
                self.invitation = None;
                self.error = Some(crate::i18n::tr("pairing.generate_code_failed").into());
            }
        } else if self
            .request
            .as_ref()
            .is_some_and(|r| !r.started && r.discovery_deadline <= now)
        {
            let manual = self
                .request
                .as_ref()
                .is_some_and(|request| request.endpoint.is_some());
            self.request = None;
            self.error = Some(
                if manual {
                    crate::i18n::tr("pairing.manual_connection_failed")
                } else {
                    crate::i18n::tr("pairing.device_not_found")
                }
                .into(),
            );
        } else if self
            .request
            .as_ref()
            .is_some_and(|r| r.attempt.expires <= now)
        {
            // A joining device has no new code to display; return it to the
            // input state and let the user enter the inviter's current code.
            self.request = None;
            self.error = Some(crate::i18n::tr("pairing.code_expired").into());
        }
    }
}

fn parse_code(code: &str) -> Result<String> {
    let code: String = code.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    if code.len() != 6 || !code.bytes().all(|c| c.is_ascii_digit()) {
        crate::i18n::localized!(bail, "pairing.invalid_code")
    }
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pairing() -> SharedPairing {
        Pairing::new(
            ConfigStore::open(
                std::env::temp_dir()
                    .join(format!(
                        "zipover-pair-test-{}",
                        hex::encode(random_bytes::<16>().unwrap())
                    ))
                    .join("devices.json"),
            )
            .unwrap(),
        )
    }
    fn peer() -> TrustedPeer {
        TrustedPeer {
            id: [9; 16],
            key: [7; 32],
            addresses: Vec::new(),
            name: "test".into(),
        }
    }

    #[test]
    fn six_digit_code_preserves_leading_zeros() {
        assert_eq!(parse_code(" 001 234\n").unwrap(), "001234");
        for invalid in [
            "12345",
            "1234567",
            "１２３４５６",
            "12a456",
            "zipover1:secret",
        ] {
            assert!(parse_code(invalid).is_err());
        }
        let a = pairing();
        assert_eq!(
            parse_code(&a.lock().unwrap().invite().unwrap())
                .unwrap()
                .len(),
            6
        );
    }

    #[test]
    fn invite_is_single_use_and_revocation_removes_trust() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.invite().unwrap();
        let attempt = a.begin_incoming().unwrap();
        a.accept(peer(), &attempt, ConnectionRole::Responder)
            .unwrap();
        assert!(
            a.accept(peer(), &attempt, ConnectionRole::Responder)
                .is_err()
        );
        assert!(a.begin_incoming().is_err());
        a.store.forget(peer().id).unwrap();
        assert!(a.key_for(peer().id).is_err());
    }

    #[test]
    fn canceled_renewed_and_expired_pairing_cannot_save_trust() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.invite().unwrap();
        let attempt = a.begin_incoming().unwrap();
        a.cancel();
        assert!(
            a.accept(peer(), &attempt, ConnectionRole::Responder)
                .is_err()
        );
        a.invite().unwrap();
        assert!(
            a.accept(peer(), &attempt, ConnectionRole::Responder)
                .is_err()
        );
        let attempt = a.begin_incoming().unwrap();
        a.invitation.as_mut().unwrap().attempt.expires = Instant::now();
        assert!(
            a.accept(peer(), &attempt, ConnectionRole::Responder)
                .is_err()
        );
        a.join("123456", None).unwrap();
        let attempt = a.request.as_ref().unwrap().attempt.clone();
        a.cancel();
        assert!(
            a.accept(peer(), &attempt, ConnectionRole::Initiator)
                .is_err()
        );
    }

    #[test]
    fn guess_budget_is_global_to_invitation() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.invite().unwrap();
        let old_generation = a.invitation.as_ref().unwrap().attempt.generation;
        let attempts: Vec<_> = (0..MAX_ATTEMPTS)
            .map(|_| a.begin_incoming().unwrap())
            .collect();
        assert!(a.begin_incoming().is_err());
        for attempt in attempts {
            a.failed(&attempt, ConnectionRole::Responder);
        }
        let invitation = a.invitation.as_ref().unwrap();
        assert_ne!(invitation.attempt.generation, old_generation);
        assert_eq!(invitation.attempts, 0);
        assert!(a.invitation_code().is_some());
        assert!(a.store.config.peers.is_empty());
        assert!(a.begin_incoming().is_ok());
    }

    #[test]
    fn expired_invitation_rotates_without_leaving_waiting_state() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.invite().unwrap();
        let old_generation = a.invitation.as_ref().unwrap().attempt.generation;
        a.invitation.as_mut().unwrap().attempt.expires = Instant::now();
        assert!(a.invitation_code().is_none());
        assert!(a.is_inviting());

        a.expire();

        let invitation = a.invitation.as_ref().unwrap();
        assert_ne!(invitation.attempt.generation, old_generation);
        assert_eq!(invitation.attempts, 0);
        assert!(invitation.attempt.expires > Instant::now());
        assert!(a.invitation_code().is_some());
        assert!(a.is_inviting());
        assert!(a.error.is_none());
        a.cancel();
        assert!(!a.is_inviting());
    }

    #[test]
    fn expired_join_returns_to_code_input() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.join("123456", None).unwrap();
        a.request.as_mut().unwrap().started = true;
        a.request.as_mut().unwrap().attempt.expires = Instant::now();

        a.expire();

        assert!(a.request.is_none());
        assert_eq!(
            a.error.as_deref(),
            Some(crate::i18n::tr("pairing.code_expired").as_ref())
        );
    }

    #[test]
    fn discovery_timeout_is_distinct_from_an_expired_code() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.join("123456", None).unwrap();
        assert_eq!(a.join_progress(), Some(JoinProgress::Discovering));
        a.request.as_mut().unwrap().discovery_deadline = Instant::now();
        a.expire();
        assert!(a.join_progress().is_none());
        assert!(
            a.error
                .as_deref()
                .unwrap()
                .starts_with(crate::i18n::tr("pairing.device_not_found").as_ref())
        );

        a.join("123456", None).unwrap();
        let request = a.request.as_mut().unwrap();
        request.started = true;
        request.discovery_deadline = Instant::now();
        a.expire();
        assert_eq!(a.join_progress(), Some(JoinProgress::Connecting));
        assert!(a.error.is_none());
    }

    #[test]
    fn manual_pairing_connects_immediately_and_cancellation_cannot_save_an_endpoint() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        let endpoint = "127.0.0.1:52134".parse().unwrap();
        a.join_at("123456", endpoint).unwrap();
        assert_eq!(a.join_progress(), Some(JoinProgress::Connecting));
        let attempt = a.request.as_ref().unwrap().attempt.clone();
        a.cancel();
        let mut remote = peer();
        remote.addresses = vec![endpoint];
        assert!(
            a.accept(remote, &attempt, ConnectionRole::Initiator)
                .is_err()
        );
        assert!(a.store.config.peers.is_empty());
        a.join_at("123456", endpoint).unwrap();
        a.request.as_mut().unwrap().discovery_deadline = Instant::now();
        a.expire();
        assert!(a.request.is_none());
        assert!(
            a.error
                .as_deref()
                .unwrap()
                .starts_with(crate::i18n::tr("pairing.manual_connection_failed").as_ref())
        );
        assert!(a.store.config.peers.is_empty());
    }
}
