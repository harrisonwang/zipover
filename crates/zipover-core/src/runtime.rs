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
    io,
    net::{SocketAddr, TcpListener},
    sync::mpsc,
    thread,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncOptions {
    /// None selects a saved or available port; explicit addresses bind strictly.
    pub listen: Option<SocketAddr>,
    pub connect: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    pub id: DeviceId,
    pub name: String,
    pub connected: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub listen_addr: Option<SocketAddr>,
    pub pairing_addresses: Vec<SocketAddr>,
    pub devices: Vec<DeviceInfo>,
    pub nearby: Vec<DeviceInfo>,
    pub code: Option<String>,
    pub seconds_left: u64,
    pub joining: Option<JoinProgress>,
    pub paired: bool,
    pub error: Option<String>,
}

pub struct SyncRuntime {
    local_addr: SocketAddr,
    hub: PeerHub,
    pairing: SharedPairing,
    clipboard_thread: Option<thread::JoinHandle<Result<()>>>,
    network_thread: Option<thread::JoinHandle<()>>,
}

impl SyncRuntime {
    pub fn start(options: SyncOptions) -> Result<Self> {
        let mut store = ConfigStore::load()?;
        let clipboard = Clipboard::new().context(crate::i18n::text(
            "访问系统剪贴板失败",
            "Unable to access the system clipboard",
        ))?;
        let listener = start_listener(&mut store, options.listen)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let discovery = match Discovery::start(&store.config, local_addr) {
            Ok(discovery) => Some(discovery),
            Err(error) => {
                crate::i18n::localized!(
                    eprintln,
                    "自动发现不可用，仍可通过 IP 地址配对或连接已保存的设备：{error:#}",
                    "Device discovery is unavailable; pair by IP address or connect to saved devices: {error:#}"
                );
                None
            }
        };
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
            local_addr,
            hub,
            pairing,
            clipboard_thread,
            network_thread,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
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
            listen_addr: Some(self.local_addr),
            pairing_addresses: crate::address::pairing_addresses(self.local_addr),
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

    pub fn join_at(&self, code: &str, endpoint: SocketAddr) -> Result<()> {
        self.pairing
            .lock()
            .expect("配对状态锁已损坏")
            .join_at(code, endpoint)
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
            .map_err(|_| {
                crate::i18n::localized!(
                    anyhow,
                    "剪贴板同步线程异常退出",
                    "The clipboard sync thread stopped unexpectedly"
                )
            })?;
        self.stop();
        result
    }
}

fn bind_listener(requested: Option<SocketAddr>, preferred: SocketAddr) -> io::Result<TcpListener> {
    if let Some(address) = requested {
        return TcpListener::bind(address);
    }
    match TcpListener::bind(preferred) {
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            TcpListener::bind(SocketAddr::new(preferred.ip(), 0))
        }
        result => result,
    }
}

fn start_listener(store: &mut ConfigStore, requested: Option<SocketAddr>) -> Result<TcpListener> {
    let preferred = SocketAddr::from((
        [0, 0, 0, 0],
        store
            .config
            .listen_port
            .filter(|port| *port != 0)
            .unwrap_or(45876),
    ));
    let listener = bind_listener(requested, preferred).with_context(|| {
        if let Some(address) = requested {
            crate::i18n::localized!(
                format,
                "监听 {address} 失败。请用 --listen 选择其他地址，或省略 --listen 自动选择可用端口",
                "Unable to listen on {address}. Choose another address with --listen, or omit --listen to select an available port automatically"
            )
        } else {
            crate::i18n::localized!(
                format,
                "无法在 {} 上选择可用的监听端口",
                "Unable to select an available listening port on {}",
                preferred.ip()
            )
        }
    })?;
    if requested.is_none() {
        store.remember_listen_port(listener.local_addr()?.port())?;
    }
    Ok(listener)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, net::UdpSocket};

    #[test]
    fn automatic_listening_recovers_from_tcp_conflicts_but_explicit_listening_does_not() {
        let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
        let preferred = occupied.local_addr().unwrap();
        let automatic = bind_listener(None, preferred).unwrap();
        let actual = automatic.local_addr().unwrap();
        assert_eq!(actual.ip(), preferred.ip());
        assert_ne!(actual.port(), preferred.port());
        assert_ne!(actual.port(), 0);
        assert_eq!(
            bind_listener(Some(preferred), preferred)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AddrInUse
        );
        let explicit = bind_listener(Some("127.0.0.1:0".parse().unwrap()), preferred).unwrap();
        assert_ne!(explicit.local_addr().unwrap().port(), 0);
    }

    #[test]
    fn udp_occupation_does_not_trigger_tcp_fallback() {
        let (tcp, udp) = (0..32)
            .find_map(|_| {
                let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
                UdpSocket::bind(tcp.local_addr().unwrap())
                    .ok()
                    .map(|udp| (tcp, udp))
            })
            .expect("a port available for both TCP and UDP");
        let preferred = udp.local_addr().unwrap();
        drop(tcp);
        let listener = bind_listener(None, preferred).unwrap();
        assert_eq!(listener.local_addr().unwrap(), preferred);
    }

    #[test]
    fn automatic_listening_does_not_hide_invalid_local_addresses() {
        let unavailable = "192.0.2.1:45876".parse().unwrap();
        let error = bind_listener(None, unavailable).unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn automatic_port_survives_restart_and_explicit_overrides_leave_it_unchanged() {
        let root = std::env::temp_dir().join(format!(
            "zipover-listen-{}",
            hex::encode(crate::config::random_bytes::<16>().unwrap())
        ));
        let path = root.join("devices.json");
        let mut store = ConfigStore::open(path.clone()).unwrap();
        let occupied = TcpListener::bind("0.0.0.0:0").unwrap();
        let preferred = occupied.local_addr().unwrap();
        store.remember_listen_port(preferred.port()).unwrap();
        let automatic = start_listener(&mut store, None).unwrap();
        let actual = automatic.local_addr().unwrap();
        assert_ne!(actual.port(), preferred.port());
        assert_eq!(store.config.listen_port, Some(actual.port()));
        let explicit = start_listener(&mut store, Some("127.0.0.1:0".parse().unwrap())).unwrap();
        assert_ne!(explicit.local_addr().unwrap().port(), actual.port());
        assert_eq!(store.config.listen_port, Some(actual.port()));
        let error = start_listener(&mut store, Some(preferred)).unwrap_err();
        assert!(error.to_string().contains("--listen"));
        assert_eq!(store.config.listen_port, Some(actual.port()));
        drop(explicit);
        drop(automatic);
        drop(store);
        let mut store = ConfigStore::open(path).unwrap();
        let listener = start_listener(&mut store, None).unwrap();
        assert_eq!(listener.local_addr().unwrap(), actual);
        drop(listener);
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }
}
