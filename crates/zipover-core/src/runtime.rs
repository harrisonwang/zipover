use crate::{
    config::{ConfigStore, DeviceConfig, DeviceId},
    discovery::Discovery,
    files::cleanup_stale_cache,
    pairing::{JoinProgress, Pairing, SharedPairing},
    sync::clipboard_loop,
    transport::{PeerHub, network_loop},
};
use anyhow::{Context, Result, anyhow};
use arboard::Clipboard;
use std::{
    net::{SocketAddr, TcpListener},
    sync::mpsc,
    thread,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncOptions {
    pub listen: SocketAddr,
    pub connect: Option<String>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            listen: ([0, 0, 0, 0], 45876).into(),
            connect: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    pub id: DeviceId,
    pub name: String,
    pub connected: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub devices: Vec<DeviceInfo>,
    pub nearby: Vec<DeviceInfo>,
    pub code: Option<String>,
    pub seconds_left: u64,
    pub joining: Option<JoinProgress>,
    pub paired: bool,
    pub error: Option<String>,
}

pub struct SyncRuntime {
    hub: PeerHub,
    pairing: SharedPairing,
    clipboard_thread: Option<thread::JoinHandle<Result<()>>>,
    network_thread: Option<thread::JoinHandle<()>>,
}

impl SyncRuntime {
    pub fn start(options: SyncOptions) -> Result<Self> {
        let store = ConfigStore::load()?;
        let clipboard = Clipboard::new().context("访问系统剪贴板失败")?;
        let listener = TcpListener::bind(options.listen).with_context(|| {
            format!("监听 {} 失败（是否已有 Zipover 在运行？）", options.listen)
        })?;
        listener.set_nonblocking(true)?;
        let discovery = Discovery::start(&store.config, listener.local_addr()?)?;
        let pairing = Pairing::new(store);
        let hub = PeerHub::default();
        let (tx, rx) = mpsc::sync_channel(32);
        let network_hub = hub.clone();
        let network_pairing = pairing.clone();
        let network_thread = Some(thread::spawn(move || {
            network_loop(
                listener,
                discovery,
                network_hub,
                network_pairing,
                tx,
                options,
            );
        }));
        let clipboard_thread = {
            cleanup_stale_cache();
            let hub = hub.clone();
            Some(thread::spawn(move || clipboard_loop(clipboard, hub, rx)))
        };
        Ok(Self {
            hub,
            pairing,
            clipboard_thread,
            network_thread,
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        let pairing = self.pairing.lock().expect("配对状态锁已损坏");
        let connected = self.hub.devices();
        let devices: Vec<_> = pairing
            .store
            .config
            .peers
            .iter()
            .map(|peer| DeviceInfo {
                id: peer.id,
                name: peer.name.clone(),
                connected: connected.contains(&peer.id),
            })
            .collect();
        let mut nearby: Vec<_> = pairing
            .discovered
            .iter()
            .filter(|peer| peer.inviting && !connected.contains(&peer.id))
            .map(|peer| DeviceInfo {
                id: peer.id,
                name: peer.name.clone(),
                connected: false,
            })
            .collect();
        nearby.sort_by_key(|peer| peer.id);
        Snapshot {
            devices,
            nearby,
            code: pairing.invitation_code().map(str::to_owned),
            seconds_left: pairing.seconds_left(),
            joining: pairing.join_progress(),
            paired: pairing.result.is_some(),
            error: pairing.error.clone(),
        }
    }

    pub fn generate_code(&self) -> Result<String> {
        self.pairing.lock().expect("配对状态锁已损坏").invite()
    }

    pub fn join(&self, code: &str, device: Option<String>) -> Result<()> {
        self.pairing
            .lock()
            .expect("配对状态锁已损坏")
            .join(code, device)
    }

    pub fn cancel_pairing(&self) {
        self.pairing.lock().expect("配对状态锁已损坏").cancel();
    }

    pub fn forget(&self, id: DeviceId) -> Result<()> {
        let mut pairing = self.pairing.lock().expect("配对状态锁已损坏");
        pairing.store.forget(id)?;
        self.hub.disconnect_device(id);
        Ok(())
    }

    pub fn stop(&self) {
        self.hub.shutdown();
    }

    pub fn wait(mut self) -> Result<()> {
        let result = self
            .clipboard_thread
            .take()
            .expect("同步运行时持有剪贴板线程")
            .join()
            .map_err(|_| anyhow!("剪贴板同步线程异常退出"))?;
        self.stop();
        result
    }
}

impl Drop for SyncRuntime {
    fn drop(&mut self) {
        self.stop();
        if let Some(thread) = self.clipboard_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.network_thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn paired_devices() -> Result<Vec<DeviceInfo>> {
    Ok(DeviceConfig::paired_devices()?
        .into_iter()
        .map(|peer| DeviceInfo {
            id: peer.id,
            name: peer.name,
            connected: false,
        })
        .collect())
}

pub fn forget_device(id: DeviceId) -> Result<()> {
    ConfigStore::load()?.forget(id)
}
