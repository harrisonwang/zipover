use crate::{
    clipboard::{CaptureCache, CapturedPayload, apply_payload, capture_payload, set_file_list},
    files::{IncomingTransfer, cleanup_cache, files_summary, send_file_transfer},
    image::{IncomingImage, send_image_transfer},
    protocol::{ClipboardPayload, WireMessage},
    transport::{IncomingEvent, PeerHub},
};
#[cfg(test)]
use crate::{
    files::{self, TransferEntry, capture_files},
    protocol::CHUNK_SIZE,
};
use anyhow::{Result, anyhow};
use arboard::Clipboard;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    sync::mpsc::Receiver,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(test)]
#[path = "acceptance.rs"]
mod acceptance;

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_MESSAGE_ID_LEN: usize = 256;
const MAX_ACTIVE_TRANSFERS: usize = 16;
const MAX_ACTIVE_TRANSFER_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MAX_ACTIVE_IMAGES: usize = 4;
const MAX_ACTIVE_IMAGE_BYTES: u64 = 256 * 1024 * 1024;
const INITIAL_SEND_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_SEND_RETRY_DELAY: Duration = Duration::from_secs(30);
const CACHE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SessionKey {
    peer_id: u64,
    message_id: String,
}

impl SessionKey {
    fn new(peer_id: u64, message_id: String) -> Self {
        Self {
            peer_id,
            message_id,
        }
    }
}

struct ActiveTransfer {
    last_activity: Instant,
    size: u64,
    transfer: IncomingTransfer,
}

struct ActiveImage {
    last_activity: Instant,
    image: IncomingImage,
    size: u64,
}

struct SendRetry {
    delay: Duration,
    after: Instant,
}

struct OutgoingSend<'scope> {
    generation: u64,
    targets: Vec<u64>,
    job: thread::ScopedJoinHandle<'scope, Result<()>>,
}

struct SyncState {
    origin: String,
    sequence: u64,
    last_fingerprint: Option<u64>,
    sent_peers: HashSet<u64>,
    send_generation: u64,
    send_retries: HashMap<u64, SendRetry>,
    last_capture_error: Option<String>,
    last_send_error: Option<String>,
    completed: SeenIds,
    active_transfers: HashMap<SessionKey, ActiveTransfer>,
    active_images: HashMap<SessionKey, ActiveImage>,
}

impl SyncState {
    fn new() -> Self {
        Self {
            origin: format!("{}-{}", std::process::id(), timestamp_nanos()),
            sequence: 0,
            last_fingerprint: None,
            sent_peers: HashSet::new(),
            send_generation: 0,
            send_retries: HashMap::new(),
            last_capture_error: None,
            last_send_error: None,
            completed: SeenIds::default(),
            active_transfers: HashMap::new(),
            active_images: HashMap::new(),
        }
    }

    fn next_id(&mut self) -> String {
        self.sequence = self.sequence.saturating_add(1);
        format!("{}-{}", self.origin, self.sequence)
    }

    fn observe_fingerprint(&mut self, fingerprint: u64) {
        if self.last_fingerprint != Some(fingerprint) {
            self.last_fingerprint = Some(fingerprint);
            self.send_generation = self.send_generation.wrapping_add(1);
            self.sent_peers.clear();
            self.send_retries.clear();
            self.last_send_error = None;
        }
    }

    fn delivery_targets(&self, peers: &[u64], now: Instant) -> Vec<u64> {
        peers
            .iter()
            .copied()
            .filter(|peer_id| {
                !self.sent_peers.contains(peer_id)
                    && self
                        .send_retries
                        .get(peer_id)
                        .is_none_or(|retry| retry.after <= now)
            })
            .collect()
    }

    fn complete_send(
        &mut self,
        generation: u64,
        targets: &[u64],
        connected: &[u64],
        result: Result<()>,
        now: Instant,
    ) {
        // 接收端可能在发送任务运行时更新剪贴板，旧任务不能改变新内容的投递状态。
        if generation != self.send_generation {
            return;
        }
        match result {
            Ok(()) => {
                self.last_send_error = None;
                for peer_id in targets.iter().filter(|id| connected.contains(id)) {
                    self.sent_peers.insert(*peer_id);
                    self.send_retries.remove(peer_id);
                }
            }
            Err(error) => {
                for peer_id in targets.iter().filter(|id| connected.contains(id)) {
                    let delay = self
                        .send_retries
                        .get(peer_id)
                        .map_or(INITIAL_SEND_RETRY_DELAY, |retry| {
                            retry.delay.saturating_mul(2).min(MAX_SEND_RETRY_DELAY)
                        });
                    self.send_retries.insert(
                        *peer_id,
                        SendRetry {
                            delay,
                            after: now + delay,
                        },
                    );
                }
                let message = format!("{error:#}");
                if self.last_send_error.as_deref() != Some(&message) {
                    crate::i18n::localized!(eprintln, "sync.send_failed", message = message);
                }
                self.last_send_error = Some(message);
            }
        }
    }
}

pub(crate) fn clipboard_loop(
    mut clipboard: Clipboard,
    hub: PeerHub,
    incoming: Receiver<IncomingEvent>,
) -> Result<()> {
    let mut state = SyncState::new();
    let mut cache = CaptureCache::default();
    let mut next_cache_cleanup = Instant::now();

    thread::scope(|scope| {
        let mut outgoing: Option<OutgoingSend<'_>> = None;
        let result = loop {
            if hub.is_shutdown() {
                break Ok(());
            }
            while let Ok(event) = incoming.try_recv() {
                if let IncomingEvent::Message { peer_id, .. } = &event
                    && !hub.is_connected(*peer_id)
                {
                    continue;
                }
                if let Err(error) = handle_incoming_event(event, &mut clipboard, &mut state) {
                    eprintln!("{error}");
                }
            }
            expire_sessions(&mut state);
            if Instant::now() >= next_cache_cleanup {
                cleanup_file_cache(&mut clipboard, &state);
                next_cache_cleanup = Instant::now() + CACHE_MAINTENANCE_INTERVAL;
            }

            if let Some(send) = outgoing.take_if(|send| send.job.is_finished()) {
                let result = send.job.join().unwrap_or_else(|_| {
                    Err(crate::i18n::localized!(
                        anyhow,
                        "sync.sending_thread_stopped"
                    ))
                });
                let peers = match hub.active_peer_ids() {
                    Ok(peers) => peers,
                    Err(error) => break Err(error),
                };
                state.complete_send(
                    send.generation,
                    &send.targets,
                    &peers,
                    result,
                    Instant::now(),
                );
            }

            if outgoing.is_none() {
                let peers = match hub.active_peer_ids() {
                    Ok(peers) => peers,
                    Err(error) => break Err(error),
                };
                if !peers.is_empty() {
                    match capture_payload(&mut clipboard, &mut cache) {
                        Ok(payload) => {
                            state.last_capture_error = None;
                            let fingerprint = captured_fingerprint(&payload);
                            state.observe_fingerprint(fingerprint);
                            let targets = state.delivery_targets(&peers, Instant::now());

                            if !targets.is_empty() {
                                let id = state.next_id();
                                let summary = captured_summary(&payload);
                                crate::i18n::localized!(
                                    eprintln,
                                    "sync.sending",
                                    summary = summary
                                );
                                // 有界发送队列产生背压时，仍继续接收。
                                let hub = &hub;
                                let send_targets = targets.clone();
                                outgoing = Some(OutgoingSend {
                                    generation: state.send_generation,
                                    targets,
                                    job: scope.spawn(move || {
                                        send_payload(hub, &send_targets, &id, payload)
                                    }),
                                });
                            }
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            if state.last_capture_error.as_deref() != Some(&message) {
                                crate::i18n::localized!(
                                    eprintln,
                                    "sync.capture_failed",
                                    message = message
                                );
                                state.last_capture_error = Some(message);
                            }
                        }
                    }
                }
            }

            thread::sleep(POLL_INTERVAL);
        };
        // 在线程作用域等待发送任务结束前，先解除套接字写入的阻塞。
        hub.shutdown();
        result
    })
}

fn send_payload(hub: &PeerHub, peers: &[u64], id: &str, payload: CapturedPayload) -> Result<()> {
    match payload {
        CapturedPayload::Text(text) => hub.send_to_targets(
            peers,
            &WireMessage::Clipboard {
                id: id.to_string(),
                payload: ClipboardPayload::Text(text),
            },
        ),
        CapturedPayload::Image(image) => send_image_transfer(hub, peers, id, &image),
        CapturedPayload::Files(files) => {
            if files.internal {
                crate::i18n::localized!(eprintln, "sync.ignored_internal_paths");
                return Ok(());
            }
            send_file_transfer(hub, peers, id, &files)
        }
    }
}

fn handle_incoming_event(
    event: IncomingEvent,
    clipboard: &mut Clipboard,
    state: &mut SyncState,
) -> Result<()> {
    let (peer_id, message) = match event {
        IncomingEvent::Message { peer_id, message } => (peer_id, message),
        IncomingEvent::Disconnected { peer_id } => {
            state.sent_peers.remove(&peer_id);
            state.send_retries.remove(&peer_id);
            cleanup_peer_sessions(state, peer_id);
            return Ok(());
        }
    };

    match message {
        WireMessage::Clipboard { id, payload } => {
            if !valid_message_id(&id) || state.completed.contains(&id) {
                return Ok(());
            }
            let fingerprint = fingerprint(&payload);
            let summary = payload_summary(&payload);
            crate::i18n::localized!(eprintln, "sync.received", summary = summary);
            apply_payload(clipboard, &payload).map_err(
                |error| crate::i18n::localized!(anyhow, "sync.apply_failed", error = error : {:#}),
            )?;
            state.completed.insert(&id);
            mark_remote_applied(state, fingerprint, peer_id);
            crate::i18n::localized!(eprintln, "sync.applied", summary = summary);
        }
        WireMessage::TransferStart { id, entries } => {
            if !valid_message_id(&id) || state.completed.contains(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id.clone());
            state.active_transfers.remove(&key);
            if state.active_transfers.len() >= MAX_ACTIVE_TRANSFERS {
                crate::i18n::localized!(eprintln, "sync.too_many_file_transfers");
                return Ok(());
            }
            let Some(transfer_size) = entries
                .iter()
                .try_fold(0u64, |total, entry| total.checked_add(entry.size))
            else {
                crate::i18n::localized!(eprintln, "sync.file_size_overflow");
                return Ok(());
            };
            let active_size = state
                .active_transfers
                .values()
                .map(|active| active.size)
                .sum::<u64>();
            if active_size.saturating_add(transfer_size) > MAX_ACTIVE_TRANSFER_BYTES {
                crate::i18n::localized!(eprintln, "sync.file_size_limit");
                return Ok(());
            }
            let cache_scope = format!("{}-{peer_id}", state.origin);
            let transfer = IncomingTransfer::start(id, &cache_scope, entries, &protected_cache_paths(clipboard, state)?).map_err(|error| {
                crate::i18n::localized!(anyhow, "sync.file_transfer_rejected", error = error : {:#})
            })?;
            crate::i18n::localized!(
                eprintln,
                "sync.receiving_files",
                summary = transfer.summary()
            );
            state.active_transfers.insert(
                key,
                ActiveTransfer {
                    last_activity: Instant::now(),
                    size: transfer_size,
                    transfer,
                },
            );
        }
        WireMessage::TransferChunk {
            id,
            file_index,
            offset,
            bytes,
        } => {
            if !valid_message_id(&id) || state.completed.contains(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id);
            let Some(active) = state.active_transfers.get_mut(&key) else {
                return Ok(());
            };
            active.last_activity = Instant::now();
            if let Err(error) = active.transfer.accept_chunk(file_index, offset, &bytes) {
                crate::i18n::localized!(eprintln, "sync.file_chunk_failed", error = error : {:#});
                state.active_transfers.remove(&key);
            }
        }
        WireMessage::TransferFileEnd {
            id,
            file_index,
            sha256,
        } => {
            if !valid_message_id(&id) || state.completed.contains(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id);
            let Some(active) = state.active_transfers.get_mut(&key) else {
                return Ok(());
            };
            active.last_activity = Instant::now();
            if let Err(error) = active.transfer.finish_file(file_index, sha256) {
                crate::i18n::localized!(eprintln, "sync.file_verification_failed", error = error : {:#});
                state.active_transfers.remove(&key);
            }
        }
        WireMessage::TransferEnd { id } => {
            if !valid_message_id(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id.clone());
            if state.completed.contains(&id) {
                state.active_transfers.remove(&key);
                return Ok(());
            }
            let Some(active) = state.active_transfers.remove(&key) else {
                crate::i18n::localized!(eprintln, "sync.file_transfer_missing", id = id);
                return Ok(());
            };
            let summary = active.transfer.summary();
            let finished = active.transfer.finish().map_err(|error| {
                crate::i18n::localized!(anyhow, "sync.file_completion_failed", error = error : {:#})
            })?;
            if let Err(error) = set_file_list(clipboard, &finished.roots) {
                finished.cleanup();
                return Err(
                    crate::i18n::localized!(anyhow, "sync.write_file_list_failed", error = error : {:#}),
                );
            }
            state.completed.insert(&id);
            mark_remote_applied(state, finished.fingerprint, peer_id);
            cleanup_file_cache(clipboard, state);
            crate::i18n::localized!(eprintln, "sync.completed", summary = summary);
        }
        WireMessage::TransferAbort { id } => {
            let key = SessionKey::new(peer_id, id);
            state.active_transfers.remove(&key);
            state.active_images.remove(&key);
            crate::i18n::localized!(eprintln, "sync.remote_aborted");
        }
        WireMessage::ImageStart {
            id,
            fingerprint,
            width,
            height,
            size,
        } => {
            if !valid_message_id(&id) || state.completed.contains(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id);
            if state.active_images.len() >= MAX_ACTIVE_IMAGES {
                crate::i18n::localized!(eprintln, "sync.too_many_image_transfers");
                return Ok(());
            }
            let active_size = state
                .active_images
                .values()
                .map(|active| active.size)
                .sum::<u64>();
            if active_size.saturating_add(size) > MAX_ACTIVE_IMAGE_BYTES {
                crate::i18n::localized!(eprintln, "sync.image_resource_limit");
                return Ok(());
            }
            let image = IncomingImage::start(fingerprint, width, height, size).map_err(|error| {
                crate::i18n::localized!(anyhow, "sync.image_transfer_rejected", error = error : {:#})
            })?;
            crate::i18n::localized!(
                eprintln,
                "sync.receiving_image",
                width = width,
                height = height,
                size = format_bytes(size)
            );
            state.active_images.insert(
                key,
                ActiveImage {
                    last_activity: Instant::now(),
                    image,
                    size,
                },
            );
        }
        WireMessage::ImageChunk { id, offset, bytes } => {
            if !valid_message_id(&id) || state.completed.contains(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id);
            let Some(active) = state.active_images.get_mut(&key) else {
                return Ok(());
            };
            active.last_activity = Instant::now();
            if let Err(error) = active.image.accept_chunk(offset, &bytes) {
                crate::i18n::localized!(eprintln, "sync.image_chunk_failed", error = error : {:#});
                state.active_images.remove(&key);
            }
        }
        WireMessage::ImageEnd { id, sha256 } => {
            if !valid_message_id(&id) {
                return Ok(());
            }
            let key = SessionKey::new(peer_id, id.clone());
            if state.completed.contains(&id) {
                state.active_images.remove(&key);
                return Ok(());
            }
            let Some(active) = state.active_images.remove(&key) else {
                crate::i18n::localized!(eprintln, "sync.image_transfer_missing", id = id);
                return Ok(());
            };
            let (fingerprint, image) = active.image.finish(sha256).map_err(|error| {
                crate::i18n::localized!(anyhow, "sync.image_completion_failed", error = error : {:#})
            })?;
            clipboard.set_image(image).map_err(|error| {
                crate::i18n::localized!(anyhow, "sync.apply_image_failed", error = error : {:#})
            })?;
            state.completed.insert(&id);
            mark_remote_applied(state, fingerprint, peer_id);
            crate::i18n::localized!(eprintln, "sync.image_applied");
        }
    }
    Ok(())
}

fn mark_remote_applied(state: &mut SyncState, fingerprint: u64, peer_id: u64) {
    state.observe_fingerprint(fingerprint);
    state.send_generation = state.send_generation.wrapping_add(1);
    state.send_retries.clear();
    state.sent_peers.insert(peer_id);
    state.last_send_error = None;
}

fn cleanup_peer_sessions(state: &mut SyncState, peer_id: u64) {
    state
        .active_transfers
        .retain(|key, _| key.peer_id != peer_id);
    state.active_images.retain(|key, _| key.peer_id != peer_id);
}

fn cleanup_file_cache(clipboard: &mut Clipboard, state: &SyncState) {
    if let Ok(protected) = protected_cache_paths(clipboard, state) {
        cleanup_cache(&protected);
    }
}

fn protected_cache_paths(
    clipboard: &mut Clipboard,
    state: &SyncState,
) -> Result<Vec<std::path::PathBuf>> {
    let mut protected = match clipboard.get().file_list() {
        Ok(paths) => paths,
        Err(arboard::Error::ContentNotAvailable) => Vec::new(),
        // 无法读取剪贴板时不能确认缓存文件是否仍被引用，保留到下次维护。
        Err(error) => {
            return Err(crate::i18n::localized!(
                anyhow,
                "files.read_clipboard_before_cleanup_failed",
                error = error
            ));
        }
    };
    protected.extend(
        state
            .active_transfers
            .values()
            .map(|active| active.transfer.cache_directory().to_path_buf()),
    );
    Ok(protected)
}

fn expire_sessions(state: &mut SyncState) {
    let now = Instant::now();
    state
        .active_transfers
        .retain(|_, active| now.duration_since(active.last_activity) < SESSION_IDLE_TIMEOUT);
    state
        .active_images
        .retain(|_, active| now.duration_since(active.last_activity) < SESSION_IDLE_TIMEOUT);
}

fn valid_message_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_MESSAGE_ID_LEN
}

fn payload_summary(payload: &ClipboardPayload) -> String {
    match payload {
        ClipboardPayload::Text(text) => text_summary(text),
        ClipboardPayload::Image {
            width,
            height,
            bytes,
        } => {
            crate::i18n::localized!(
                format,
                "image.summary",
                width = width,
                height = height,
                size = format_bytes(bytes.len() as u64)
            )
        }
    }
}

fn text_summary(text: &str) -> String {
    crate::i18n::localized!(
        format,
        "sync.text_summary",
        size = format_bytes(text.len() as u64)
    )
}

fn captured_summary(payload: &CapturedPayload) -> String {
    match payload {
        CapturedPayload::Text(text) => text_summary(text),
        CapturedPayload::Image(image) => image.summary(),
        CapturedPayload::Files(files) => files_summary(files),
    }
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        return format!("{bytes} B");
    }
    let value = format!("{value:.2}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned();
    format!("{value} {}", UNITS[unit])
}

fn captured_fingerprint(payload: &CapturedPayload) -> u64 {
    match payload {
        CapturedPayload::Text(text) => text_fingerprint(text),
        CapturedPayload::Image(image) => image.fingerprint(),
        CapturedPayload::Files(files) => files.identity,
    }
}

fn fingerprint(payload: &ClipboardPayload) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match payload {
        ClipboardPayload::Text(text) => {
            0u8.hash(&mut hasher);
            text.hash(&mut hasher);
        }
        ClipboardPayload::Image {
            width,
            height,
            bytes,
        } => {
            1u8.hash(&mut hasher);
            width.hash(&mut hasher);
            height.hash(&mut hasher);
            bytes.hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn text_fingerprint(text: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    0u8.hash(&mut hasher);
    text.hash(&mut hasher);
    hasher.finish()
}

fn timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[derive(Default)]
struct SeenIds {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenIds {
    fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    fn insert(&mut self, id: &str) -> bool {
        if !self.set.insert(id.to_owned()) {
            return false;
        }
        self.order.push_back(id.to_owned());
        if self.order.len() > 256
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{env, fs, path::PathBuf};

    #[test]
    fn relative_path_rejects_traversal() {
        assert!(files::safe_relative_path(b"../secret.txt").is_err());
        assert_eq!(
            files::safe_relative_path(b"folder\\document.txt").unwrap(),
            PathBuf::from("folder/document.txt")
        );
    }

    #[test]
    fn directory_identity_changes_when_child_changes() {
        let root = env::temp_dir().join(format!("zipover-sync-test-{}", timestamp_nanos()));
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("nested").join("child.txt"), b"before").unwrap();
        let before = capture_files(std::slice::from_ref(&root)).unwrap().identity;
        fs::write(root.join("nested").join("child.txt"), b"after").unwrap();
        let after = capture_files(std::slice::from_ref(&root)).unwrap().identity;
        assert_ne!(before, after);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn file_chunks_verify_and_commit_atomically() {
        let entry = TransferEntry {
            path: b"sample.txt".to_vec(),
            kind: files::TransferEntryKind::File,
            size: 11,
            mode: files::file_mode(&fs::metadata(env::temp_dir()).unwrap()),
        };
        let mut transfer =
            IncomingTransfer::start(format!("test-{}", timestamp_nanos()), "0", vec![entry], &[])
                .unwrap();
        transfer.accept_chunk(0, 0, b"hello world").unwrap();
        transfer
            .finish_file(0, Sha256::digest(b"hello world").into())
            .unwrap();
        let finished = transfer.finish().unwrap();
        assert_eq!(fs::read(&finished.roots[0]).unwrap(), b"hello world");
        finished.cleanup();
    }

    #[test]
    fn file_transfer_preserves_dot_parts_files_and_directories() {
        for name in [".parts/received.txt", ".parts"] {
            let mut entries = Vec::new();
            if name.contains('/') {
                entries.push(TransferEntry {
                    path: b".parts".to_vec(),
                    kind: files::TransferEntryKind::Directory,
                    size: 0,
                    mode: None,
                });
            }
            entries.push(TransferEntry {
                path: name.as_bytes().to_vec(),
                kind: files::TransferEntryKind::File,
                size: 7,
                mode: None,
            });
            let mut transfer =
                IncomingTransfer::start(format!("parts-{}", timestamp_nanos()), "0", entries, &[])
                    .unwrap();
            transfer.accept_chunk(0, 0, b"payload").unwrap();
            transfer
                .finish_file(0, Sha256::digest(b"payload").into())
                .unwrap();
            let finished = transfer.finish().unwrap();
            let received = if name.contains('/') {
                finished.roots[0].join("received.txt")
            } else {
                finished.roots[0].clone()
            };
            assert_eq!(fs::read(received).unwrap(), b"payload");
            finished.cleanup();
        }
    }

    #[test]
    fn sync_state_generates_unique_message_ids() {
        let mut state = SyncState::new();
        assert_ne!(state.next_id(), state.next_id());
    }

    #[test]
    fn remote_clipboard_resets_delivery_tracking() {
        let mut state = SyncState::new();
        state.observe_fingerprint(1);
        state.sent_peers.extend([2, 3]);

        mark_remote_applied(&mut state, 2, 4);
        assert_eq!(state.sent_peers, HashSet::from([4]));

        mark_remote_applied(&mut state, 2, 5);
        assert_eq!(state.sent_peers, HashSet::from([4, 5]));
    }

    #[test]
    fn transient_send_failure_retries_without_resending_successful_peers() {
        let mut state = SyncState::new();
        let now = Instant::now();
        state.observe_fingerprint(1);
        let generation = state.send_generation;
        state.complete_send(generation, &[2], &[2, 3], Ok(()), now);
        assert_eq!(state.delivery_targets(&[2, 3], now), vec![3]);

        state.complete_send(
            generation,
            &[3],
            &[2, 3],
            Err(anyhow!("temporary file read failure")),
            now,
        );
        assert!(state.delivery_targets(&[2, 3], now).is_empty());
        let retry_at = now + INITIAL_SEND_RETRY_DELAY;
        assert_eq!(state.delivery_targets(&[2, 3], retry_at), vec![3]);
        state.complete_send(generation, &[3], &[2, 3], Ok(()), retry_at);
        assert!(state.delivery_targets(&[2, 3], retry_at).is_empty());
        assert!(state.send_retries.is_empty());
    }

    #[test]
    fn repeated_failures_back_off_until_content_changes() {
        let mut state = SyncState::new();
        let mut now = Instant::now();
        state.observe_fingerprint(1);
        for delay in [1, 2, 4, 8, 16, 30, 30] {
            state.complete_send(
                state.send_generation,
                &[2],
                &[2],
                Err(anyhow!("temporarily unavailable")),
                now,
            );
            now += Duration::from_secs(delay);
            assert!(
                state
                    .delivery_targets(&[2], now - Duration::from_millis(1))
                    .is_empty()
            );
            assert_eq!(state.delivery_targets(&[2], now), vec![2]);
        }
        state.complete_send(
            state.send_generation,
            &[2],
            &[2],
            Err(anyhow!("temporarily unavailable")),
            now,
        );
        state.observe_fingerprint(2);
        assert_eq!(state.delivery_targets(&[2], now), vec![2]);
        assert!(state.last_send_error.is_none());
    }

    #[test]
    fn old_send_results_cannot_overwrite_remote_clipboard_delivery() {
        for result in [Ok(()), Err(anyhow!("old transfer failed"))] {
            let mut state = SyncState::new();
            state.observe_fingerprint(1);
            let generation = state.send_generation;
            mark_remote_applied(&mut state, 2, 4);
            state.complete_send(generation, &[2, 3], &[2, 3, 4], result, Instant::now());
            assert_eq!(state.last_fingerprint, Some(2));
            assert_eq!(state.sent_peers, HashSet::from([4]));
            assert!(state.send_retries.is_empty());
            assert!(state.last_send_error.is_none());
        }
    }

    #[test]
    fn completed_broadcast_only_marks_targets_still_connected() {
        let mut state = SyncState::new();
        state.observe_fingerprint(1);
        state.complete_send(state.send_generation, &[2, 3], &[3], Ok(()), Instant::now());
        assert_eq!(state.sent_peers, HashSet::from([3]));
    }

    #[test]
    fn captured_text_matches_wire_text_fingerprint() {
        let text = "hello";
        assert_eq!(
            text_fingerprint(text),
            fingerprint(&ClipboardPayload::Text(text.to_owned()))
        );
    }

    #[test]
    fn format_bytes_uses_decimal_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1_000), "1 KB");
        assert_eq!(format_bytes(1_250_000), "1.25 MB");
    }
}
