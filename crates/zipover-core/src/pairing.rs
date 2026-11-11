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
    // 同一邀请码可有多个并发握手；序号区分实际提交信任的那一次连接。
    connection: u8,
    pub code: String,
    expires: Instant,
}

struct Invitation {
    attempt: PairAttempt,
    attempts: u8,
}

pub(crate) struct PairRequest {
    attempt: PairAttempt,
    pub device: Option<String>,
    pub endpoint: Option<SocketAddr>,
    started: bool,
    discovery_deadline: Instant,
}

enum PairingState {
    Idle,
    Inviting(Invitation),
    Joining(PairRequest),
    Confirming {
        attempt: PairAttempt,
        role: ConnectionRole,
    },
    Paired,
    Failed(String),
}

pub(crate) struct Pairing {
    pub store: ConfigStore,
    pub discovered: Vec<DiscoveredPeer>,
    state: PairingState,
}

pub(crate) type SharedPairing = Arc<Mutex<Pairing>>;

impl Pairing {
    pub fn new(store: ConfigStore) -> SharedPairing {
        let mut pairing = Self {
            store,
            discovered: Vec::new(),
            state: PairingState::Idle,
        };
        // 首次启动直接进入待配对状态，另一台设备打开后即可发现本机。
        if pairing.store.config.peers.is_empty() {
            pairing.renew_invitation();
        }
        Arc::new(Mutex::new(pairing))
    }

    pub fn invite(&mut self) -> Result<String> {
        self.cancel();
        self.start_invitation()
    }

    fn start_invitation(&mut self) -> Result<String> {
        // 拒绝采样既保留前导零，也避免取模偏差。
        let number = loop {
            let number = u32::from_le_bytes(random_bytes()?);
            if number < u32::MAX - u32::MAX % 1_000_000 {
                break number % 1_000_000;
            }
        };
        let code = format!("{number:06}");
        let attempt = PairAttempt {
            generation: random_bytes()?,
            connection: 0,
            code: code.clone(),
            expires: Instant::now() + PAIRING_LIFETIME,
        };
        self.state = PairingState::Inviting(Invitation {
            attempt,
            attempts: 0,
        });
        Ok(code)
    }

    fn renew_invitation(&mut self) {
        if self.start_invitation().is_err() {
            self.state =
                PairingState::Failed(crate::i18n::tr("pairing.generate_code_failed").into());
        }
    }

    pub fn is_inviting(&self) -> bool {
        // 更新配对码时不能短暂撤销设备发现广播。
        matches!(self.state, PairingState::Inviting(_))
    }

    pub fn invitation_code(&self) -> Option<&str> {
        match &self.state {
            PairingState::Inviting(invitation) if invitation.attempt.expires > Instant::now() => {
                Some(&invitation.attempt.code)
            }
            _ => None,
        }
    }

    pub fn seconds_left(&self) -> u64 {
        match &self.state {
            PairingState::Inviting(invitation) => invitation
                .attempt
                .expires
                .saturating_duration_since(Instant::now())
                .as_secs(),
            _ => 0,
        }
    }

    pub fn join_progress(&self) -> Option<JoinProgress> {
        match &self.state {
            PairingState::Joining(request) => {
                Some(if request.started || request.endpoint.is_some() {
                    JoinProgress::Connecting
                } else {
                    JoinProgress::Discovering
                })
            }
            PairingState::Confirming {
                role: ConnectionRole::Initiator,
                ..
            } => Some(JoinProgress::Connecting),
            _ => None,
        }
    }

    pub fn is_paired(&self) -> bool {
        matches!(self.state, PairingState::Paired)
    }

    pub fn error(&self) -> Option<&str> {
        match &self.state {
            PairingState::Failed(error) => Some(error),
            _ => None,
        }
    }

    pub(crate) fn pending_request(&self) -> Option<&PairRequest> {
        match &self.state {
            PairingState::Joining(request) if !request.started => Some(request),
            _ => None,
        }
    }

    pub(crate) fn begin_outgoing(&mut self) -> Option<PairAttempt> {
        match &mut self.state {
            PairingState::Joining(request) if !request.started => {
                request.started = true;
                Some(request.attempt.clone())
            }
            _ => None,
        }
    }

    pub(crate) fn fail_discovery(&mut self, error: String) {
        if self.pending_request().is_some() {
            self.state = PairingState::Failed(error);
        }
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
            connection: 0,
            code,
            expires: Instant::now() + PAIRING_LIFETIME,
        };
        self.state = PairingState::Joining(PairRequest {
            attempt,
            device,
            endpoint,
            started: false,
            discovery_deadline: Instant::now() + DISCOVERY_TIMEOUT,
        });
        Ok(())
    }

    pub fn cancel(&mut self) {
        self.state = PairingState::Idle;
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
        let PairingState::Inviting(invitation) = &mut self.state else {
            crate::i18n::localized!(bail, "pairing.invitation_expired");
        };
        if invitation.attempts >= MAX_ATTEMPTS {
            crate::i18n::localized!(bail, "pairing.too_many_attempts")
        }
        invitation.attempts += 1;
        let mut attempt = invitation.attempt.clone();
        attempt.connection = invitation.attempts;
        Ok(attempt)
    }

    // 仅在 SPAKE2 和加密握手确认配对码后调用。
    pub(crate) fn accept(
        &mut self,
        peer: TrustedPeer,
        attempt: &PairAttempt,
        role: ConnectionRole,
    ) -> Result<()> {
        let current = match (&self.state, role) {
            (PairingState::Joining(request), ConnectionRole::Initiator) => Some(&request.attempt),
            (PairingState::Inviting(invitation), ConnectionRole::Responder) => {
                Some(&invitation.attempt)
            }
            _ => None,
        };
        if !current
            .is_some_and(|a| a.generation == attempt.generation && a.expires > Instant::now())
        {
            crate::i18n::localized!(bail, "pairing.attempt_expired")
        }
        self.store.trust(peer)?;
        // 信任已落盘但双方尚未确认；保留尝试身份，防止迟到结果覆盖新操作。
        self.state = PairingState::Confirming {
            attempt: attempt.clone(),
            role,
        };
        Ok(())
    }

    pub(crate) fn complete(&mut self, attempt: &PairAttempt, role: ConnectionRole) -> Result<()> {
        if !matches!(&self.state, PairingState::Confirming { attempt: current, role: current_role }
            if *current_role == role
                && current.generation == attempt.generation
                && current.connection == attempt.connection
                && current.expires > Instant::now())
        {
            crate::i18n::localized!(bail, "pairing.attempt_expired");
        }
        self.state = PairingState::Paired;
        Ok(())
    }

    pub(crate) fn failed(&mut self, attempt: &PairAttempt, role: ConnectionRole) {
        match &self.state {
            PairingState::Joining(request)
                if role == ConnectionRole::Initiator
                    && request.attempt.generation == attempt.generation =>
            {
                self.state =
                    PairingState::Failed(crate::i18n::tr("pairing.connection_failed").into());
            }
            PairingState::Confirming {
                attempt: current,
                role: current_role,
            } if *current_role == role
                && current.generation == attempt.generation
                && current.connection == attempt.connection =>
            {
                self.state =
                    PairingState::Failed(crate::i18n::tr("pairing.connection_failed").into());
            }
            PairingState::Inviting(invitation)
                if role == ConnectionRole::Responder
                    && invitation.attempt.generation == attempt.generation
                    && invitation.attempts >= MAX_ATTEMPTS =>
            {
                self.renew_invitation();
            }
            _ => {}
        }
    }

    pub fn expire(&mut self) {
        let now = Instant::now();
        match &self.state {
            PairingState::Inviting(invitation) if invitation.attempt.expires <= now => {
                // 轮换短期配对码时持续保留邀请广播。
                self.renew_invitation();
            }
            PairingState::Joining(request)
                if !request.started && request.discovery_deadline <= now =>
            {
                self.state = PairingState::Failed(
                    if request.endpoint.is_some() {
                        crate::i18n::tr("pairing.manual_connection_failed")
                    } else {
                        crate::i18n::tr("pairing.device_not_found")
                    }
                    .into(),
                );
            }
            PairingState::Joining(PairRequest { attempt, .. })
            | PairingState::Confirming { attempt, .. }
                if attempt.expires <= now =>
            {
                self.state = PairingState::Failed(crate::i18n::tr("pairing.code_expired").into());
            }
            _ => {}
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

    fn start_attempt(pairing: &mut Pairing, role: ConnectionRole) -> PairAttempt {
        match role {
            ConnectionRole::Initiator => {
                pairing.join("123456", None).unwrap();
                pairing.begin_outgoing().unwrap()
            }
            ConnectionRole::Responder => {
                pairing.invite().unwrap();
                pairing.begin_incoming().unwrap()
            }
        }
    }

    #[test]
    fn saved_pairing_completes_only_after_confirmation_of_the_current_attempt() {
        for role in [ConnectionRole::Initiator, ConnectionRole::Responder] {
            let shared = pairing();
            let mut pairing = shared.lock().unwrap();
            let attempt = start_attempt(&mut pairing, role);
            assert!(pairing.complete(&attempt, role).is_err());
            pairing.accept(peer(), &attempt, role).unwrap();
            assert!(!pairing.is_paired());
            assert!(pairing.invitation_code().is_none());
            assert!(pairing.begin_incoming().is_err());
            assert!(pairing.accept(peer(), &attempt, role).is_err());
            assert_eq!(
                pairing.join_progress(),
                (role == ConnectionRole::Initiator).then_some(JoinProgress::Connecting)
            );
            let other_role = match role {
                ConnectionRole::Initiator => ConnectionRole::Responder,
                ConnectionRole::Responder => ConnectionRole::Initiator,
            };
            pairing.failed(&attempt, other_role);
            assert!(pairing.complete(&attempt, other_role).is_err());
            assert!(pairing.error().is_none());
            pairing.complete(&attempt, role).unwrap();
            assert!(pairing.is_paired());
            assert!(pairing.join_progress().is_none());
            assert!(pairing.complete(&attempt, role).is_err());
            pairing.failed(&attempt, role);
            assert!(pairing.is_paired());
            assert!(pairing.error().is_none());
            assert_eq!(pairing.key_for(peer().id).unwrap(), peer().key);
        }
    }

    #[test]
    fn late_confirmation_and_failure_cannot_overwrite_cancel_or_replacement() {
        let replacements: [fn(&mut Pairing); 3] = [
            Pairing::cancel,
            |pairing| {
                pairing.invite().unwrap();
            },
            |pairing| {
                pairing.join("654321", None).unwrap();
            },
        ];
        for role in [ConnectionRole::Initiator, ConnectionRole::Responder] {
            for replace in replacements {
                let shared = pairing();
                let mut pairing = shared.lock().unwrap();
                let attempt = start_attempt(&mut pairing, role);
                pairing.accept(peer(), &attempt, role).unwrap();
                replace(&mut pairing);
                let code = pairing.invitation_code().map(str::to_owned);
                let generation = pairing
                    .pending_request()
                    .map(|request| request.attempt.generation);
                let progress = pairing.join_progress();

                assert!(pairing.complete(&attempt, role).is_err());
                pairing.failed(&attempt, role);

                assert_eq!(pairing.invitation_code(), code.as_deref());
                assert_eq!(
                    pairing
                        .pending_request()
                        .map(|request| request.attempt.generation),
                    generation
                );
                assert_eq!(pairing.join_progress(), progress);
                assert!(!pairing.is_paired());
                assert!(pairing.error().is_none());
                assert_eq!(pairing.key_for(peer().id).unwrap(), peer().key);
            }
        }
    }

    #[test]
    fn failed_or_expired_confirmation_leaves_a_visible_error() {
        for role in [ConnectionRole::Initiator, ConnectionRole::Responder] {
            for expired in [false, true] {
                let shared = pairing();
                let mut pairing = shared.lock().unwrap();
                let attempt = start_attempt(&mut pairing, role);
                pairing.accept(peer(), &attempt, role).unwrap();
                let error = if expired {
                    let PairingState::Confirming {
                        attempt: current, ..
                    } = &mut pairing.state
                    else {
                        panic!()
                    };
                    current.expires = Instant::now();
                    assert!(pairing.complete(&attempt, role).is_err());
                    pairing.expire();
                    crate::i18n::tr("pairing.code_expired")
                } else {
                    pairing.failed(&attempt, role);
                    crate::i18n::tr("pairing.connection_failed")
                };
                assert_eq!(pairing.error(), Some(error.as_ref()));
                assert!(!pairing.is_paired());
                assert!(!pairing.is_inviting());
                assert!(pairing.join_progress().is_none());
                assert!(pairing.complete(&attempt, role).is_err());
                assert_eq!(pairing.key_for(peer().id).unwrap(), peer().key);
            }
        }
    }

    #[test]
    fn other_handshakes_for_the_same_code_cannot_finish_the_accepted_connection() {
        let shared = pairing();
        let mut pairing = shared.lock().unwrap();
        pairing.invite().unwrap();
        let attempts: Vec<_> = (0..MAX_ATTEMPTS)
            .map(|_| pairing.begin_incoming().unwrap())
            .collect();
        let accepted = &attempts[0];
        pairing
            .accept(peer(), accepted, ConnectionRole::Responder)
            .unwrap();
        for attempt in &attempts[1..] {
            assert!(
                pairing
                    .accept(peer(), attempt, ConnectionRole::Responder)
                    .is_err()
            );
            pairing.failed(attempt, ConnectionRole::Responder);
            assert!(
                pairing
                    .complete(attempt, ConnectionRole::Responder)
                    .is_err()
            );
            assert!(!pairing.is_paired());
            assert!(pairing.error().is_none());
        }
        pairing
            .complete(accepted, ConnectionRole::Responder)
            .unwrap();
        assert!(pairing.is_paired());
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
        let PairingState::Inviting(invitation) = &mut a.state else {
            panic!()
        };
        invitation.attempt.expires = Instant::now();
        assert!(
            a.accept(peer(), &attempt, ConnectionRole::Responder)
                .is_err()
        );
        a.join("123456", None).unwrap();
        let attempt = a.pending_request().unwrap().attempt.clone();
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
        let PairingState::Inviting(invitation) = &a.state else {
            panic!()
        };
        let old_generation = invitation.attempt.generation;
        let attempts: Vec<_> = (0..MAX_ATTEMPTS)
            .map(|_| a.begin_incoming().unwrap())
            .collect();
        assert!(a.begin_incoming().is_err());
        for attempt in attempts {
            a.failed(&attempt, ConnectionRole::Responder);
        }
        let PairingState::Inviting(invitation) = &a.state else {
            panic!()
        };
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
        let PairingState::Inviting(invitation) = &mut a.state else {
            panic!()
        };
        let old_generation = invitation.attempt.generation;
        invitation.attempt.expires = Instant::now();
        assert!(a.invitation_code().is_none());
        assert!(a.is_inviting());

        a.expire();

        let PairingState::Inviting(invitation) = &a.state else {
            panic!()
        };
        assert_ne!(invitation.attempt.generation, old_generation);
        assert_eq!(invitation.attempts, 0);
        assert!(invitation.attempt.expires > Instant::now());
        assert!(a.invitation_code().is_some());
        assert!(a.is_inviting());
        assert!(a.error().is_none());
        a.cancel();
        assert!(!a.is_inviting());
    }

    #[test]
    fn expired_join_returns_to_code_input() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.join("123456", None).unwrap();
        a.begin_outgoing().unwrap();
        let PairingState::Joining(request) = &mut a.state else {
            panic!()
        };
        request.attempt.expires = Instant::now();

        a.expire();

        assert!(a.pending_request().is_none());
        assert_eq!(
            a.error(),
            Some(crate::i18n::tr("pairing.code_expired").as_ref())
        );
    }

    #[test]
    fn discovery_timeout_is_distinct_from_an_expired_code() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        a.join("123456", None).unwrap();
        assert_eq!(a.join_progress(), Some(JoinProgress::Discovering));
        let PairingState::Joining(request) = &mut a.state else {
            panic!()
        };
        request.discovery_deadline = Instant::now();
        a.expire();
        assert!(a.join_progress().is_none());
        assert!(
            a.error()
                .unwrap()
                .starts_with(crate::i18n::tr("pairing.device_not_found").as_ref())
        );

        a.join("123456", None).unwrap();
        a.begin_outgoing().unwrap();
        let PairingState::Joining(request) = &mut a.state else {
            panic!()
        };
        request.discovery_deadline = Instant::now();
        a.expire();
        assert_eq!(a.join_progress(), Some(JoinProgress::Connecting));
        assert!(a.error().is_none());
    }

    #[test]
    fn manual_pairing_connects_immediately_and_cancellation_cannot_save_an_endpoint() {
        let a = pairing();
        let mut a = a.lock().unwrap();
        let endpoint = "127.0.0.1:52134".parse().unwrap();
        a.join_at("123456", endpoint).unwrap();
        assert_eq!(a.join_progress(), Some(JoinProgress::Connecting));
        let attempt = a.pending_request().unwrap().attempt.clone();
        a.cancel();
        let mut remote = peer();
        remote.addresses = vec![endpoint];
        assert!(
            a.accept(remote, &attempt, ConnectionRole::Initiator)
                .is_err()
        );
        assert!(a.store.config.peers.is_empty());
        a.join_at("123456", endpoint).unwrap();
        let PairingState::Joining(request) = &mut a.state else {
            panic!()
        };
        request.discovery_deadline = Instant::now();
        a.expire();
        assert!(a.pending_request().is_none());
        assert!(
            a.error()
                .unwrap()
                .starts_with(crate::i18n::tr("pairing.manual_connection_failed").as_ref())
        );
        assert!(a.store.config.peers.is_empty());
    }
}
