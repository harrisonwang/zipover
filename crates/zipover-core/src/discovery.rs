use super::pairing::{DiscoveredPeer, SharedPairing};
use crate::{config::DeviceConfig, protocol::PROTOCOL_VERSION};
use anyhow::Result;
use mdns_sd::{
    DaemonEvent, IfKind, Receiver, ResolvedService, ScopedIp, ServiceDaemon, ServiceEvent,
    ServiceInfo,
};
use std::net::{SocketAddr, SocketAddrV6};

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
        let ip = (!address.ip().is_unspecified()).then_some(address.ip());
        let mut service = ServiceInfo::new(
            SERVICE_TYPE,
            &id,
            &format!("zipover-{id}.local."),
            ip.as_slice(),
            address.port(),
            &properties[..],
        )?;
        if ip.is_none() {
            service = service.enable_addr_auto();
        }
        service.set_interfaces(vec![if address.is_ipv4() {
            IfKind::IPv4
        } else {
            IfKind::All
        }]);
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
                    Err(error) => crate::i18n::localized!(
                        eprintln,
                        "自动发现：{error}",
                        "Device discovery: {error}"
                    ),
                }
            }
        }
        for event in self.monitor.try_iter() {
            if let DaemonEvent::Error(error) = event {
                crate::i18n::localized!(eprintln, "自动发现：{error}", "Device discovery: {error}");
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
                    let addresses = resolved_addresses(&info);
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
                        crate::i18n::localized!(
                            eprintln,
                            "自动发现：{} {:?}",
                            "Device discovery: {} {:?}",
                            peer.name,
                            peer.addresses
                        );
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

fn resolved_addresses(info: &ResolvedService) -> Vec<SocketAddr> {
    let mut addresses: Vec<_> = info
        .get_addresses()
        .iter()
        .filter_map(|ip| match ip {
            ScopedIp::V4(ip) => Some(SocketAddr::from((*ip.addr(), info.get_port()))),
            ScopedIp::V6(ip) => Some(SocketAddr::V6(SocketAddrV6::new(
                *ip.addr(),
                info.get_port(),
                0,
                if ip.addr().is_unicast_link_local() {
                    ip.scope_id().index
                } else {
                    0
                },
            ))),
            _ => None,
        })
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    addresses
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

#[cfg(test)]
#[path = "discovery/tests.rs"]
mod tests;
