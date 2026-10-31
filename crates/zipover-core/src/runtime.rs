use crate::{
    config::{ConfigStore, DeviceConfig, DeviceId},
    discovery::Discovery,
    pairing::{JoinProgress, Pairing, SharedPairing},
    sync::clipboard_loop,
    transport::{PeerHub, network_loop},
};
use anyhow::{Context, Result, anyhow};
use arboard::Clipboard;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex, mpsc},
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
            listen: ([0u16; 8], 57321).into(),
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
    pub listen_addr: Option<SocketAddr>,
    pub pairing_addresses: Vec<SocketAddr>,
    pub devices: Vec<DeviceInfo>,
    pub nearby: Vec<DeviceInfo>,
    pub code: Option<String>,
    pub seconds_left: u64,
    pub joining: Option<JoinProgress>,
    pub paired: bool,
    pub error: Option<String>,
    pub discovery_error: Option<String>,
    pub sync_error: Option<String>,
    pub sending_files: bool,
}

#[derive(Default)]
pub(crate) struct RuntimeStatus {
    pub discovery_error: Option<String>,
    pub sync_error: Option<String>,
    pub sending_files: bool,
}

pub(crate) type SharedStatus = Arc<Mutex<RuntimeStatus>>;

pub struct SyncRuntime {
    local_addr: SocketAddr,
    hub: PeerHub,
    pairing: SharedPairing,
    status: SharedStatus,
    clipboard_thread: Option<thread::JoinHandle<Result<()>>>,
    network_thread: Option<thread::JoinHandle<()>>,
}

impl SyncRuntime {
    pub fn start(options: SyncOptions) -> Result<Self> {
        let store = ConfigStore::load()?;
        let clipboard = Clipboard::new().context(crate::i18n::tr("clipboard.unavailable"))?;
        let listener = bind_listener(options.listen).with_context(|| {
            crate::i18n::localized!(format, "runtime.listen_failed", address = options.listen)
        })?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let status = SharedStatus::default();
        let discovery = match Discovery::start(&store.config, local_addr, status.clone()) {
            Ok(discovery) => Some(discovery),
            Err(error) => {
                crate::i18n::localized!(eprintln, "runtime.discovery_unavailable", error = error : {:#});
                status
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .discovery_error = Some(
                    crate::i18n::localized!(format, "runtime.discovery_unavailable", error = error : {:#}),
                );
                None
            }
        };
        let local_id = store.config.id;
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
            let hub = hub.clone();
            let status = status.clone();
            Some(thread::spawn(move || {
                clipboard_loop(clipboard, hub, rx, local_id, status)
            }))
        };
        Ok(Self {
            local_addr,
            hub,
            pairing,
            status,
            clipboard_thread,
            network_thread,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn snapshot(&self) -> Snapshot {
        // 枚举网卡属于系统查询，不占用配对与配置的共享锁。
        let pairing_addresses = crate::address::pairing_addresses(self.local_addr);
        let pairing = self.pairing.lock().unwrap_or_else(|error| {
            panic!("{}: {error:?}", crate::i18n::tr("pairing.lock_poisoned"))
        });
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
        let status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Snapshot {
            listen_addr: Some(self.local_addr),
            pairing_addresses,
            devices,
            nearby,
            code: pairing.invitation_code().map(str::to_owned),
            seconds_left: pairing.seconds_left(),
            joining: pairing.join_progress(),
            paired: pairing.result.is_some(),
            error: pairing.error.clone(),
            discovery_error: status.discovery_error.clone(),
            sync_error: status.sync_error.clone(),
            sending_files: status.sending_files,
        }
    }

    pub fn generate_code(&self) -> Result<String> {
        self.pairing
            .lock()
            .unwrap_or_else(|error| {
                panic!("{}: {error:?}", crate::i18n::tr("pairing.lock_poisoned"))
            })
            .invite()
    }

    pub fn join(&self, code: &str, device: Option<String>) -> Result<()> {
        self.pairing
            .lock()
            .unwrap_or_else(|error| {
                panic!("{}: {error:?}", crate::i18n::tr("pairing.lock_poisoned"))
            })
            .join(code, device)
    }

    pub fn join_at(&self, code: &str, endpoint: SocketAddr) -> Result<()> {
        self.pairing
            .lock()
            .unwrap_or_else(|error| {
                panic!("{}: {error:?}", crate::i18n::tr("pairing.lock_poisoned"))
            })
            .join_at(code, endpoint)
    }

    pub fn cancel_pairing(&self) {
        self.pairing
            .lock()
            .unwrap_or_else(|error| {
                panic!("{}: {error:?}", crate::i18n::tr("pairing.lock_poisoned"))
            })
            .cancel();
    }

    pub fn forget(&self, id: DeviceId) -> Result<()> {
        let mut pairing = self.pairing.lock().unwrap_or_else(|error| {
            panic!("{}: {error:?}", crate::i18n::tr("pairing.lock_poisoned"))
        });
        pairing.store.forget(id)?;
        self.hub.disconnect_device(id);
        Ok(())
    }

    pub fn stop(&self) {
        self.hub.shutdown();
    }

    pub fn wait(mut self) -> Result<()> {
        self.clipboard_thread
            .take()
            .unwrap_or_else(|| panic!("{}", crate::i18n::tr("runtime.clipboard_thread_missing")))
            .join()
            .map_err(|_| crate::i18n::localized!(anyhow, "runtime.clipboard_thread_stopped"))?
    }
}

pub(crate) fn bind_listener(address: SocketAddr) -> io::Result<TcpListener> {
    if address.is_ipv4() {
        return TcpListener::bind(address);
    }
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    // 各操作系统的默认值不同，因此在此显式设置。
    socket.set_only_v6(false)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;
    Ok(socket.into())
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
    use std::{
        io::{Read, Write},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpStream},
        time::Duration,
    };

    #[test]
    fn default_listener_accepts_ipv4_and_ipv6_on_the_same_port() {
        let mut address = SyncOptions::default().listen;
        address.set_port(0);
        let listener = bind_listener(address).unwrap();
        let port = listener.local_addr().unwrap().port();
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ] {
            let mut client =
                TcpStream::connect_timeout(&SocketAddr::new(ip, port), Duration::from_secs(2))
                    .unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            client.write_all(b"dual-stack").unwrap();
            let (mut server, _) = listener.accept().unwrap();
            server
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut message = [0; 10];
            server.read_exact(&mut message).unwrap();
            assert_eq!(&message, b"dual-stack");
            server.write_all(b"ok").unwrap();
            let mut reply = [0; 2];
            client.read_exact(&mut reply).unwrap();
            assert_eq!(&reply, b"ok");
        }
    }
}
