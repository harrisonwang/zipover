use crate::config::{DeviceId, PairingKey, TrustedPeer};
use crate::{
    crypto::{self, ConnectionRole},
    discovery::Discovery,
    pairing::{PairAttempt, SharedPairing},
    protocol::{FRAME_LIMIT, WireMessage},
    runtime::SyncOptions,
    sync::format_bytes,
};
use anyhow::{Context, Result, anyhow, bail};
use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

pub(crate) enum IncomingEvent {
    Message { peer_id: u64, message: WireMessage },
    Disconnected { peer_id: u64 },
}
const MAX_PEERS: usize = 32;
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_SEND_TIMEOUT: Duration = Duration::from_secs(5);

const OUTGOING_QUEUE_SIZE: usize = 16;
static NEXT_PEER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(Clone)]
pub(crate) struct PeerHub {
    peers: Arc<Mutex<Vec<Peer>>>,
    shutdown: Arc<AtomicBool>,
}

impl Default for PeerHub {
    fn default() -> Self {
        Self {
            peers: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct Peer {
    peer_id: u64,
    device_id: DeviceId,
    sender: SyncSender<Arc<[u8]>>,
    alive: Arc<AtomicBool>,
    stream: Arc<Mutex<TcpStream>>,
}

impl PeerHub {
    pub(crate) fn add(
        &self,
        peer_id: u64,
        device_id: DeviceId,
        sender: SyncSender<Arc<[u8]>>,
        alive: Arc<AtomicBool>,
        stream: Arc<Mutex<TcpStream>>,
    ) -> Result<bool> {
        if !alive.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let mut peers = self.peers.lock().map_err(|_| {
            crate::i18n::localized!(
                anyhow,
                "设备列表锁已损坏",
                "The device list lock is poisoned"
            )
        })?;
        peers.retain(|peer| peer.alive.load(Ordering::Relaxed));
        if self.is_shutdown()
            || peers.len() >= MAX_PEERS
            || peers.iter().any(|peer| peer.device_id == device_id)
        {
            return Ok(false);
        }
        peers.push(Peer {
            peer_id,
            device_id,
            sender,
            alive,
            stream,
        });
        Ok(true)
    }

    pub(crate) fn active_peer_ids(&self) -> Result<Vec<u64>> {
        let mut peers = self.peers.lock().map_err(|_| {
            crate::i18n::localized!(
                anyhow,
                "设备列表锁已损坏",
                "The device list lock is poisoned"
            )
        })?;
        peers.retain(|peer| peer.alive.load(Ordering::Relaxed));
        Ok(peers.iter().map(|peer| peer.peer_id).collect())
    }

    pub(crate) fn devices(&self) -> Vec<DeviceId> {
        self.peers
            .lock()
            .expect("设备列表锁已损坏")
            .iter()
            .filter(|peer| peer.alive.load(Ordering::Relaxed))
            .map(|peer| peer.device_id)
            .collect()
    }

    pub(crate) fn is_connected(&self, id: u64) -> bool {
        self.peers
            .lock()
            .expect("设备列表锁已损坏")
            .iter()
            .any(|peer| peer.peer_id == id && peer.alive.load(Ordering::Acquire))
    }

    pub(crate) fn disconnect_device(&self, id: DeviceId) {
        let mut peers = self.peers.lock().expect("设备列表锁已损坏");
        peers.retain(|peer| {
            if peer.device_id != id {
                return true;
            }
            peer.alive.store(false, Ordering::Release);
            let _ = peer
                .stream
                .lock()
                .expect("设备连接锁已损坏")
                .shutdown(Shutdown::Both);
            false
        });
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.disconnect_all();
    }

    pub(crate) fn disconnect_all(&self) {
        let peers = self.peers.lock().expect("设备列表锁已损坏");
        for peer in peers.iter() {
            peer.alive.store(false, Ordering::Release);
            let _ = peer
                .stream
                .lock()
                .expect("设备连接锁已损坏")
                .shutdown(Shutdown::Both);
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    fn remove(&self, peer_id: u64) -> Result<()> {
        let mut peers = self.peers.lock().map_err(|_| {
            crate::i18n::localized!(
                anyhow,
                "设备列表锁已损坏",
                "The device list lock is poisoned"
            )
        })?;
        peers.retain(|peer| peer.peer_id != peer_id);
        Ok(())
    }

    pub(crate) fn send_to_targets(&self, peer_ids: &[u64], message: &WireMessage) -> Result<()> {
        if peer_ids.is_empty() {
            crate::i18n::localized!(bail, "当前没有已连接的设备", "No devices are connected");
        }
        let frame = encode_message(message)?;
        let targets = {
            let peers = self.peers.lock().map_err(|_| {
                crate::i18n::localized!(
                    anyhow,
                    "设备列表锁已损坏",
                    "The device list lock is poisoned"
                )
            })?;
            peer_ids
                .iter()
                .filter_map(|peer_id| {
                    peers
                        .iter()
                        .find(|peer| peer.peer_id == *peer_id)
                        .map(|peer| (peer.sender.clone(), peer.alive.clone()))
                })
                .collect::<Vec<_>>()
        };
        let mut sent = false;
        for (sender, alive) in targets {
            if !alive.load(Ordering::Relaxed) {
                continue;
            }
            match sender.send(frame.clone()) {
                Ok(()) => sent = true,
                Err(_) => {
                    alive.store(false, Ordering::Relaxed);
                }
            }
        }
        let mut peers = self.peers.lock().map_err(|_| {
            crate::i18n::localized!(
                anyhow,
                "设备列表锁已损坏",
                "The device list lock is poisoned"
            )
        })?;
        peers.retain(|peer| peer.alive.load(Ordering::Relaxed));
        if !sent {
            crate::i18n::localized!(bail, "设备连接已关闭", "The device connection is closed");
        }
        Ok(())
    }
}

fn encode_message(message: &WireMessage) -> Result<Arc<[u8]>> {
    let frame = bincode::serialize(message).context(crate::i18n::text(
        "序列化剪贴板消息失败",
        "Unable to encode the clipboard message",
    ))?;
    if frame.is_empty() || frame.len() > FRAME_LIMIT {
        crate::i18n::localized!(
            bail,
            "剪贴板消息过大，无法放入单帧（{}）",
            "The clipboard message exceeds the frame size limit ({})",
            format_bytes(frame.len() as u64)
        );
    }
    Ok(Arc::from(frame.into_boxed_slice()))
}

// A fixed preface routes to the right pairwise key; Noise binds the entire
// preface into its transcript, so changing either identity or mode fails auth.
const MAGIC: &[u8; 5] = b"ZPOV2";
const PREFACE_LEN: usize = 38;

pub(crate) fn network_loop(
    listener: TcpListener,
    mut discovery: Option<Discovery>,
    hub: PeerHub,
    shared: SharedPairing,
    incoming: SyncSender<IncomingEvent>,
    options: SyncOptions,
) {
    let mut jobs: Vec<(DeviceId, thread::JoinHandle<()>)> = Vec::new();
    let mut next_connect = Instant::now();
    while !hub.is_shutdown() {
        if let Some(discovery) = &mut discovery {
            discovery.poll(&shared);
        }
        {
            let mut pairing = shared.lock().expect("配对状态锁已损坏");
            pairing.expire();
        }
        jobs.retain(|(_, job)| !job.is_finished());
        if jobs.len() < MAX_PEERS {
            match listener.accept() {
                Ok((stream, _)) => {
                    let hub = hub.clone();
                    let shared = shared.clone();
                    let incoming = incoming.clone();
                    jobs.push((
                        [0; 16],
                        thread::spawn(move || {
                            if let Err(error) = establish(stream, None, &hub, &shared, &incoming) {
                                crate::i18n::localized!(
                                    eprintln,
                                    "剪贴板同步：拒绝连接：{error:#}",
                                    "Clipboard sync: connection rejected: {error:#}"
                                );
                            }
                        }),
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => crate::i18n::localized!(
                    eprintln,
                    "剪贴板同步：接受连接失败：{error}",
                    "Clipboard sync: unable to accept connection: {error}"
                ),
            }
        }
        if Instant::now() >= next_connect {
            next_connect = Instant::now() + Duration::from_secs(2);
            let active = hub.devices();
            let mut pairing = shared.lock().expect("配对状态锁已损坏");
            let local_id = pairing.store.config.id;
            let mut targets: Vec<(DeviceId, ConnectionAuth, PeerAddresses)> = Vec::new();
            for trusted in &pairing.store.config.peers {
                let mut addresses = Vec::new();
                if (local_id < trusted.id || !trusted.addresses.is_empty())
                    && let Some(peer) = pairing.discovered.iter().find(|peer| peer.id == trusted.id)
                {
                    addresses.extend_from_slice(&peer.addresses);
                }
                for address in &trusted.addresses {
                    if !addresses.contains(address) {
                        addresses.push(*address);
                    }
                }
                addresses
                    .retain(|address| crate::address::validate_pairing_address(*address).is_ok());
                if !addresses.is_empty() {
                    targets.push((
                        trusted.id,
                        ConnectionAuth::Trusted(trusted.key),
                        PeerAddresses::Discovered(addresses),
                    ));
                }
            }
            let state = &mut *pairing;
            if let Some(request) = state.request.as_mut().filter(|r| !r.started) {
                let mut candidates: Vec<_> = state
                    .discovered
                    .iter()
                    .filter(|p| {
                        p.inviting
                            && request
                                .device
                                .as_ref()
                                .is_none_or(|d| *d == p.name || *d == hex::encode(p.id))
                    })
                    .collect();
                let already_connected = candidates.iter().any(|peer| active.contains(&peer.id));
                candidates.retain(|peer| !active.contains(&peer.id));
                let target = if let Some(address) = request.endpoint {
                    Some(([0; 16], PeerAddresses::Discovered(vec![address])))
                } else if let Some(address) = &options.connect {
                    Some(([0; 16], PeerAddresses::Explicit(address.clone())))
                } else if candidates.len() == 1 {
                    Some((
                        candidates[0].id,
                        PeerAddresses::Discovered(candidates[0].addresses.clone()),
                    ))
                } else {
                    None
                };
                if let Some((id, addresses)) = target {
                    if jobs.len() < MAX_PEERS && !jobs.iter().any(|(pending, _)| *pending == id) {
                        request.started = true;
                        targets.retain(|(other, ..)| *other != id);
                        targets.insert(
                            0,
                            (id, ConnectionAuth::Pair(request.attempt.clone()), addresses),
                        );
                    }
                } else if candidates.len() > 1 {
                    state.request = None;
                    state.error = Some(
                        crate::i18n::text(
                            "发现多台待配对设备，请选择设备后重试",
                            "Multiple devices are ready to pair. Select a device and try again.",
                        )
                        .into(),
                    );
                } else if already_connected {
                    state.request = None;
                    state.error = Some(
                        crate::i18n::text(
                            "此设备已连接，无需重复配对",
                            "This device is already connected. Pairing again is not needed.",
                        )
                        .into(),
                    );
                }
            } else if state.request.is_none()
                && let Some(address) = &options.connect
                && state.store.config.peers.len() == 1
            {
                let peer = &state.store.config.peers[0];
                targets.retain(|(other, ..)| *other != peer.id);
                targets.push((
                    peer.id,
                    ConnectionAuth::Trusted(peer.key),
                    PeerAddresses::Explicit(address.clone()),
                ));
            }
            drop(pairing);
            for (id, auth, addresses) in targets {
                if (active.contains(&id) && matches!(auth, ConnectionAuth::Trusted(_)))
                    || jobs.iter().any(|(pending, _)| *pending == id)
                    || jobs.len() >= MAX_PEERS
                {
                    continue;
                }
                let hub = hub.clone();
                let shared = shared.clone();
                let incoming = incoming.clone();
                jobs.push((
                    id,
                    thread::spawn(move || {
                        let result = match &auth {
                            ConnectionAuth::Trusted(key) => connect_trusted_addresses(
                                addresses, id, *key, &hub, &shared, &incoming,
                            ),
                            ConnectionAuth::Pair(_) => {
                                connect_addresses(addresses).and_then(|stream| {
                                    establish(
                                        stream,
                                        Some((id, auth.clone())),
                                        &hub,
                                        &shared,
                                        &incoming,
                                    )
                                })
                            }
                        };
                        if let Err(error) = result {
                            crate::i18n::localized!(
                                eprintln,
                                "剪贴板同步：连接 {} 失败：{error:#}",
                                "Clipboard sync: unable to connect to {}: {error:#}",
                                hex::encode(id)
                            );
                            if let ConnectionAuth::Pair(attempt) = &auth {
                                shared
                                    .lock()
                                    .expect("配对状态锁已损坏")
                                    .failed(attempt, ConnectionRole::Initiator);
                            }
                        }
                    }),
                ));
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    for (_, job) in jobs {
        let _ = job.join();
    }
}

enum PeerAddresses {
    Discovered(Vec<SocketAddr>),
    Explicit(String),
}

#[cfg(test)]
#[path = "transport/manual_tests.rs"]
mod manual_tests;

fn resolve_addresses(addresses: PeerAddresses) -> Result<Vec<SocketAddr>> {
    Ok(match addresses {
        PeerAddresses::Discovered(addresses) => addresses,
        PeerAddresses::Explicit(address) => match crate::parse_pairing_address(&address) {
            Ok(address) => vec![address],
            Err(_) => address
                .to_socket_addrs()
                .with_context(|| {
                    crate::i18n::localized!(
                        format,
                        "解析设备地址 {address} 失败",
                        "Unable to resolve device address {address}"
                    )
                })?
                .collect(),
        },
    })
}

fn connect_trusted_addresses(
    addresses: PeerAddresses,
    id: DeviceId,
    key: PairingKey,
    hub: &PeerHub,
    shared: &SharedPairing,
    incoming: &SyncSender<IncomingEvent>,
) -> Result<()> {
    let mut last_error = None;
    for address in resolve_addresses(addresses)? {
        if hub.is_shutdown() {
            break;
        }
        let result = TcpStream::connect_timeout(&address, Duration::from_secs(1))
            .map_err(anyhow::Error::from)
            .and_then(|stream| {
                establish(
                    stream,
                    Some((id, ConnectionAuth::Trusted(key))),
                    hub,
                    shared,
                    incoming,
                )
            });
        match result {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        crate::i18n::localized!(
            anyhow,
            "没有可用的设备地址",
            "No device addresses are available"
        )
    }))
}

fn connect_addresses(addresses: PeerAddresses) -> Result<TcpStream> {
    let mut last_error = None;
    for address in resolve_addresses(addresses)? {
        match TcpStream::connect_timeout(&address, Duration::from_secs(1)) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.map(anyhow::Error::from).unwrap_or_else(|| {
        crate::i18n::localized!(
            anyhow,
            "没有可用的设备地址",
            "No device addresses are available"
        )
    }))
}

#[derive(Clone)]
enum ConnectionAuth {
    Trusted(PairingKey),
    Pair(PairAttempt),
}

fn establish(
    mut stream: TcpStream,
    target: Option<(DeviceId, ConnectionAuth)>,
    hub: &PeerHub,
    shared: &SharedPairing,
    incoming: &SyncSender<IncomingEvent>,
) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(PEER_CONNECT_TIMEOUT))?;
    stream.set_write_timeout(Some(PEER_SEND_TIMEOUT))?;
    stream.set_nodelay(true)?;
    let (local_id, local_name) = {
        let pairing = shared.lock().expect("配对状态锁已损坏");
        (pairing.store.config.id, pairing.store.config.name.clone())
    };
    let mut preface = [0; PREFACE_LEN];
    let (role, mut id, auth) = if let Some((id, auth)) = target {
        preface[..5].copy_from_slice(MAGIC);
        preface[5..21].copy_from_slice(&local_id);
        preface[21..37].copy_from_slice(&id);
        // Mode 1 was the old long-code flow. Never interpret it as a numeric code.
        preface[37] = if matches!(auth, ConnectionAuth::Pair(_)) {
            2
        } else {
            0
        };
        stream.write_all(&preface)?;
        (ConnectionRole::Initiator, id, auth)
    } else {
        stream.read_exact(&mut preface)?;
        let pairing_mode = preface[37] == 2;
        if &preface[..5] != MAGIC
            || !matches!(preface[37], 0 | 2)
            || (preface[21..37] != local_id && !(pairing_mode && preface[21..37] == [0; 16]))
        {
            crate::i18n::localized!(
                bail,
                "协议或目标设备不匹配，请更新两端 Zipover",
                "The protocol or target device does not match. Update Zipover on both computers."
            )
        }
        let id: DeviceId = preface[5..21].try_into().expect("固定长度 ID");
        let mut pairing = shared.lock().expect("配对状态锁已损坏");
        let auth = if pairing_mode {
            ConnectionAuth::Pair(pairing.begin_incoming()?)
        } else {
            ConnectionAuth::Trusted(pairing.key_for(id)?)
        };
        (ConnectionRole::Responder, id, auth)
    };
    let result = (|| -> Result<()> {
        if id == local_id {
            crate::i18n::localized!(
                bail,
                "不能连接本机",
                "You cannot connect this computer to itself"
            )
        }
        let mut context = preface.to_vec();
        let key = match &auth {
            ConnectionAuth::Trusted(key) => *key,
            ConnectionAuth::Pair(attempt) => {
                // Return the responder ID for the explicit-address fallback; bind it
                // into both PAKE and Noise even when the caller did not know it.
                let mut responder = local_id;
                if role == ConnectionRole::Initiator {
                    stream.read_exact(&mut responder)?;
                    if id != [0; 16] && id != responder {
                        crate::i18n::localized!(
                            bail,
                            "对端设备不匹配",
                            "The remote device does not match"
                        )
                    }
                    id = responder;
                    if id == local_id {
                        crate::i18n::localized!(
                            bail,
                            "不能连接本机",
                            "You cannot connect this computer to itself"
                        )
                    }
                } else {
                    stream.write_all(&responder)?;
                }
                context.extend_from_slice(&responder);
                crypto::pairing_key(&mut stream, role, &attempt.code, &context)?
            }
        };
        let (mut writer, mut reader) = crypto::handshake(&mut stream, role, &key, &context)?;
        writer.write_frame(&mut stream, local_name.as_bytes())?;
        let remote_name = String::from_utf8(reader.read_frame(&mut stream)?).context(
            crate::i18n::text("设备名不是 UTF-8", "The device name is not valid UTF-8"),
        )?;
        if remote_name.len() > 255 || remote_name.chars().any(char::is_control) {
            crate::i18n::localized!(bail, "设备名无效", "Invalid device name")
        }
        if let ConnectionAuth::Pair(attempt) = &auth {
            let peer = TrustedPeer {
                id,
                key,
                name: remote_name.clone(),
                // Only outbound targets are listening endpoints. The incoming
                // TCP source port belongs to a temporary client socket.
                addresses: if role == ConnectionRole::Initiator {
                    vec![stream.peer_addr()?]
                } else {
                    Vec::new()
                },
            };
            if role == ConnectionRole::Initiator {
                if reader.read_frame(&mut stream)? != b"paired" {
                    crate::i18n::localized!(
                        bail,
                        "对端未保存配对",
                        "The remote device did not save the pairing"
                    )
                }
                shared
                    .lock()
                    .expect("配对状态锁已损坏")
                    .accept(peer, attempt, role)?;
                writer.write_frame(&mut stream, b"paired")?;
            } else {
                shared
                    .lock()
                    .expect("配对状态锁已损坏")
                    .accept(peer, attempt, role)?;
                writer.write_frame(&mut stream, b"paired")?;
                if reader.read_frame(&mut stream)? != b"paired" {
                    crate::i18n::localized!(
                        bail,
                        "对端未保存配对",
                        "The remote device did not save the pairing"
                    )
                }
            }
            let mut pairing = shared.lock().expect("配对状态锁已损坏");
            pairing.result = Some(crate::i18n::localized!(
                format,
                "已配对：{remote_name}",
                "Paired with {remote_name}"
            ));
            pairing.error = None;
            crate::i18n::localized!(
                eprintln,
                "剪贴板同步：已配对 {remote_name}，以后自动连接",
                "Clipboard sync: paired with {remote_name}; future connections are automatic"
            );
            return Ok(());
        }
        let pairing = shared.lock().expect("配对状态锁已损坏");
        if pairing.key_for(id)? != key {
            crate::i18n::localized!(bail, "设备已解除配对", "The device is no longer paired")
        }
        stream.set_read_timeout(None)?;
        register_connection(
            stream,
            id,
            remote_name,
            writer,
            reader,
            hub.clone(),
            incoming.clone(),
        )
    })();
    if result.is_err()
        && let ConnectionAuth::Pair(attempt) = &auth
    {
        shared
            .lock()
            .expect("配对状态锁已损坏")
            .failed(attempt, role);
    }
    result
}

fn register_connection(
    stream: TcpStream,
    device_id: DeviceId,
    name: String,
    mut cipher_writer: crypto::Cipher,
    mut cipher_reader: crypto::Cipher,
    hub: PeerHub,
    incoming: SyncSender<IncomingEvent>,
) -> Result<()> {
    let (outgoing_tx, outgoing_rx) = mpsc::sync_channel::<Arc<[u8]>>(OUTGOING_QUEUE_SIZE);
    let peer_id = NEXT_PEER_ID.fetch_add(1, Ordering::Relaxed);
    let alive = Arc::new(AtomicBool::new(true));
    let stream_for_shutdown = Arc::new(Mutex::new(stream.try_clone()?));
    let mut writer = stream.try_clone()?;
    let mut reader = stream;
    if !hub.add(
        peer_id,
        device_id,
        outgoing_tx,
        alive.clone(),
        stream_for_shutdown,
    )? {
        return Ok(());
    }
    crate::i18n::localized!(
        eprintln,
        "剪贴板同步：已加密连接 {name}（{}）",
        "Clipboard sync: encrypted connection to {name} ({})",
        hex::encode(device_id)
    );
    let writer_alive = alive.clone();
    thread::spawn(move || {
        while writer_alive.load(Ordering::Relaxed) {
            let Ok(frame) = outgoing_rx.recv() else { break };
            if let Err(error) = cipher_writer.write_frame(&mut writer, &frame) {
                crate::i18n::localized!(
                    eprintln,
                    "剪贴板同步：发送数据失败：{error:#}",
                    "Clipboard sync: unable to send data: {error:#}"
                );
                break;
            }
        }
        writer_alive.store(false, Ordering::Relaxed);
        let _ = writer.shutdown(Shutdown::Both);
    });
    thread::spawn(move || {
        while let Ok(frame) = cipher_reader.read_frame(&mut reader) {
            let message = match bincode::deserialize::<WireMessage>(&frame) {
                Ok(message) => message,
                Err(error) => {
                    crate::i18n::localized!(
                        eprintln,
                        "剪贴板同步：无效消息：{error}",
                        "Clipboard sync: invalid message: {error}"
                    );
                    break;
                }
            };
            if incoming
                .send(IncomingEvent::Message { peer_id, message })
                .is_err()
            {
                break;
            }
        }
        alive.store(false, Ordering::Relaxed);
        let _ = hub.remove(peer_id);
        let _ = reader.shutdown(Shutdown::Both);
        let _ = incoming.send(IncomingEvent::Disconnected { peer_id });
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{ConfigStore, random_bytes},
        pairing::Pairing,
    };

    #[test]
    fn only_one_connection_per_device_is_registered() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        let stream = Arc::new(Mutex::new(stream));
        let hub = PeerHub::default();
        let (tx, _rx) = mpsc::sync_channel(1);
        let alive = Arc::new(AtomicBool::new(true));
        assert!(
            hub.add(1, [1; 16], tx.clone(), alive.clone(), stream.clone())
                .unwrap()
        );
        assert!(!hub.add(2, [1; 16], tx, alive, stream).unwrap());
        assert_eq!(hub.devices(), vec![[1; 16]]);
        hub.shutdown();
        drop(client);
    }

    #[test]
    fn unpaired_connection_cannot_register_or_save_trust() {
        let root = std::env::temp_dir().join(format!(
            "zipover-untrusted-{}",
            hex::encode(random_bytes::<16>().unwrap())
        ));
        let a = Pairing::new(ConfigStore::open(root.join("a/devices.json")).unwrap());
        let b = Pairing::new(ConfigStore::open(root.join("b/devices.json")).unwrap());
        let id = b.lock().unwrap().store.config.id;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_state = b.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let hub = PeerHub::default();
            let (tx, _rx) = mpsc::sync_channel(1);
            assert!(establish(stream, None, &hub, &server_state, &tx).is_err());
            assert!(hub.devices().is_empty());
        });
        let stream = TcpStream::connect(address).unwrap();
        let hub = PeerHub::default();
        let (tx, _rx) = mpsc::sync_channel(1);
        assert!(
            establish(
                stream,
                Some((id, ConnectionAuth::Trusted([0; 32]))),
                &hub,
                &a,
                &tx
            )
            .is_err()
        );
        server.join().unwrap();
        assert!(a.lock().unwrap().store.config.peers.is_empty());
        assert!(b.lock().unwrap().store.config.peers.is_empty());
        drop((a, b));
        std::fs::remove_dir_all(root).unwrap();
    }

    fn pair_over_tcp(
        a: &SharedPairing,
        b: &SharedPairing,
        code: &str,
        known_target: bool,
    ) -> (bool, bool) {
        a.lock().unwrap().join(code, None).unwrap();
        let attempt = a.lock().unwrap().request.as_ref().unwrap().attempt.clone();
        let target_id = if known_target {
            b.lock().unwrap().store.config.id
        } else {
            [0; 16]
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let b = b.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let (tx, _rx) = mpsc::sync_channel(1);
            establish(stream, None, &PeerHub::default(), &b, &tx).is_ok()
        });
        let (tx, _rx) = mpsc::sync_channel(1);
        let accepted = establish(
            TcpStream::connect(address).unwrap(),
            Some((target_id, ConnectionAuth::Pair(attempt))),
            &PeerHub::default(),
            a,
            &tx,
        )
        .is_ok();
        (accepted, server.join().unwrap())
    }

    #[test]
    fn numeric_pairing_rejects_wrong_code_then_saves_strong_keys_and_consumes_code() {
        let root = std::env::temp_dir().join(format!(
            "zipover-pin-{}",
            hex::encode(random_bytes::<16>().unwrap())
        ));
        let a = Pairing::new(ConfigStore::open(root.join("a/devices.json")).unwrap());
        let b = Pairing::new(ConfigStore::open(root.join("b/devices.json")).unwrap());
        let code = b.lock().unwrap().invite().unwrap();
        let wrong = if code == "000000" { "000001" } else { "000000" };
        assert_eq!(pair_over_tcp(&a, &b, wrong, true), (false, false));
        assert!(a.lock().unwrap().store.config.peers.is_empty());
        assert!(b.lock().unwrap().store.config.peers.is_empty());
        // Direct address fallback has no target ID until the authenticated exchange.
        assert_eq!(pair_over_tcp(&a, &b, &code, false), (true, true));
        let key = a.lock().unwrap().store.config.peers[0].key;
        assert_eq!(key, b.lock().unwrap().store.config.peers[0].key);
        assert!(b.lock().unwrap().invitation_code().is_none());
        assert_eq!(pair_over_tcp(&a, &b, &code, true), (false, false));
        assert_eq!(a.lock().unwrap().store.config.peers[0].key, key);
        drop((a, b));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn adding_another_device_preserves_existing_pairing() {
        let root = std::env::temp_dir().join(format!(
            "zipover-multiple-peers-{}",
            hex::encode(random_bytes::<16>().unwrap())
        ));
        let a = Pairing::new(ConfigStore::open(root.join("a/devices.json")).unwrap());
        let b = Pairing::new(ConfigStore::open(root.join("b/devices.json")).unwrap());
        let c = Pairing::new(ConfigStore::open(root.join("c/devices.json")).unwrap());
        let code = b.lock().unwrap().invite().unwrap();
        assert_eq!(pair_over_tcp(&a, &b, &code, true), (true, true));
        let b_id = b.lock().unwrap().store.config.id;
        let first_key = a.lock().unwrap().key_for(b_id).unwrap();

        let code = a.lock().unwrap().invite().unwrap();
        assert_eq!(pair_over_tcp(&c, &a, &code, true), (true, true));
        let c_id = c.lock().unwrap().store.config.id;
        let a_id = a.lock().unwrap().store.config.id;
        assert_eq!(a.lock().unwrap().store.config.peers.len(), 2);
        assert_eq!(a.lock().unwrap().key_for(b_id).unwrap(), first_key);
        assert_eq!(b.lock().unwrap().key_for(a_id).unwrap(), first_key);
        assert_eq!(
            a.lock().unwrap().key_for(c_id).unwrap(),
            c.lock().unwrap().key_for(a_id).unwrap()
        );
        drop((a, b, c));
        std::fs::remove_dir_all(root).unwrap();
    }
}
