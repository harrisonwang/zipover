use super::pairing::{DiscoveredPeer, SharedPairing};
use crate::{config::DeviceConfig, protocol::PROTOCOL_VERSION};
use anyhow::Result;
use mdns_sd::{DaemonEvent, IfKind, Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::net::SocketAddr;

const SERVICE_TYPE: &str = "_zipover._tcp.local.";

pub(crate) struct Discovery {
    daemon: ServiceDaemon,
    receiver: Receiver<ServiceEvent>,
    monitor: Receiver<DaemonEvent>,
    address: SocketAddr,
    inviting: bool,
}

impl Discovery {
    pub fn start(config: &DeviceConfig, address: SocketAddr) -> Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let monitor = daemon.monitor()?;
        let service = Self::service(config, address, false)?;
        let receiver = daemon.browse(SERVICE_TYPE)?;
        daemon.register(service)?;
        Ok(Self {
            daemon,
            receiver,
            monitor,
            address,
            inviting: false,
        })
    }

    fn service(config: &DeviceConfig, address: SocketAddr, inviting: bool) -> Result<ServiceInfo> {
        let id = hex::encode(config.id);
        let version = PROTOCOL_VERSION.to_string();
        let properties = [
            ("id", id.as_str()),
            ("name", config.name.as_str()),
            ("protocol", version.as_str()),
            ("pairing", if inviting { "digits-v1" } else { "" }),
        ];
        let mut service = ServiceInfo::new(
            SERVICE_TYPE,
            &id,
            &format!("zipover-{id}.local."),
            (),
            address.port(),
            &properties[..],
        )?;
        if address.ip().is_unspecified() {
            service = service.enable_addr_auto();
        } else {
            service = ServiceInfo::new(
                SERVICE_TYPE,
                &id,
                &format!("zipover-{id}.local."),
                address.ip(),
                address.port(),
                &properties[..],
            )?;
        }
        service.set_interfaces(vec![IfKind::IPv4]);
        Ok(service)
    }

    pub fn poll(&mut self, shared: &SharedPairing) {
        {
            let pairing = shared.lock().expect("配对状态锁已损坏");
            let inviting = pairing.is_inviting();
            if inviting != self.inviting {
                let result = Self::service(&pairing.store.config, self.address, inviting)
                    .and_then(|service| self.daemon.register(service).map_err(Into::into));
                match result {
                    Ok(()) => {
                        self.inviting = inviting;
                    }
                    Err(error) => eprintln!("自动发现：{error}"),
                }
            }
        }
        for event in self.monitor.try_iter() {
            if let DaemonEvent::Error(error) = event {
                eprintln!("自动发现：{error}");
            }
        }
        for event in self.receiver.try_iter() {
            let mut pairing = shared.lock().expect("配对状态锁已损坏");
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    if info
                        .get_property_val_str("protocol")
                        .and_then(|v| v.parse::<u16>().ok())
                        != Some(PROTOCOL_VERSION)
                    {
                        continue;
                    }
                    let Some(value) = info.get_property_val_str("id") else {
                        continue;
                    };
                    let mut id = [0; 16];
                    if hex::decode_to_slice(value, &mut id).is_err()
                        || id == pairing.store.config.id
                    {
                        continue;
                    }
                    let mut addresses: Vec<_> = info
                        .get_addresses_v4()
                        .into_iter()
                        .map(|ip| SocketAddr::from((ip, info.get_port())))
                        .collect();
                    addresses.sort();
                    if addresses.is_empty() {
                        continue;
                    }
                    let peer = DiscoveredPeer {
                        id,
                        name: info
                            .get_property_val_str("name")
                            .unwrap_or("Zipover")
                            .to_string(),
                        addresses,
                        inviting: info.get_property_val_str("pairing") == Some("digits-v1"),
                    };
                    if !pairing.discovered.iter().any(|old| old == &peer) {
                        eprintln!("自动发现：{} {:?}", peer.name, peer.addresses);
                    }
                    pairing.discovered.retain(|old| old.id != id);
                    pairing.discovered.push(peer);
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    if let Some(id) = fullname.strip_suffix(&format!(".{SERVICE_TYPE}")) {
                        pairing.discovered.retain(|peer| hex::encode(peer.id) != id);
                    }
                }
                _ => {}
            }
        }
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}
