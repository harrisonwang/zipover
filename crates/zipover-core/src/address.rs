use anyhow::{Context, Result, bail};
use std::net::{IpAddr, SocketAddr, SocketAddrV6};

const DEFAULT_PORT: u16 = 57321;

/// Parse a manually entered IP address, optionally followed by a listening port.
pub fn parse_pairing_address(value: &str) -> Result<SocketAddr> {
    let value = value.trim();
    let address = value
        .parse::<SocketAddr>()
        .ok()
        .or_else(|| {
            value
                .parse::<IpAddr>()
                .ok()
                .map(|ip| SocketAddr::new(ip, DEFAULT_PORT))
        })
        .or_else(|| scoped_v6(value))
        .context(crate::i18n::text(
            "请输入有效的 IP 地址，可附加端口；IPv6 端口写作 [地址]:端口",
            "Enter a valid IP address, optionally with a port; use [address]:port for IPv6",
        ))?;
    validate_pairing_address(address)?;
    Ok(address)
}

pub(crate) fn validate_pairing_address(address: SocketAddr) -> Result<()> {
    if address.port() == 0 || address.ip().is_unspecified() || address.ip().is_multicast() {
        crate::i18n::localized!(
            bail,
            "请输入设备的单播 IP 地址和非零端口",
            "Enter the computer's unicast IP address and a nonzero port"
        );
    }
    Ok(())
}

fn scoped_v6(value: &str) -> Option<SocketAddr> {
    let (host, port) = if let Some(value) = value.strip_prefix('[') {
        let (host, suffix) = value.split_once(']')?;
        let port = if suffix.is_empty() {
            DEFAULT_PORT
        } else {
            suffix.strip_prefix(':')?.parse().ok()?
        };
        (host, port)
    } else {
        (value, DEFAULT_PORT)
    };
    let (ip, zone) = host.rsplit_once('%')?;
    let scope = zone.parse::<u32>().ok().or_else(|| {
        if_addrs::get_if_addrs()
            .ok()?
            .into_iter()
            .find(|interface| interface.name == zone)
            .and_then(|interface| interface.index)
    })?;
    Some(SocketAddr::V6(SocketAddrV6::new(
        ip.parse().ok()?,
        port,
        0,
        scope,
    )))
}

pub(crate) fn pairing_addresses(listener: SocketAddr) -> Vec<SocketAddr> {
    if !listener.ip().is_unspecified() {
        return vec![listener];
    }
    let mut addresses: Vec<_> = if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter(|interface| interface.is_oper_up() && !interface.is_loopback())
        .filter_map(|interface| interface_address(listener, interface.ip(), interface.index))
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    addresses
}

fn interface_address(listener: SocketAddr, ip: IpAddr, index: Option<u32>) -> Option<SocketAddr> {
    if ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_loopback()
        || listener.is_ipv4() != ip.is_ipv4()
    {
        return None;
    }
    Some(match ip {
        IpAddr::V6(ip) if ip.is_unicast_link_local() => {
            SocketAddr::V6(SocketAddrV6::new(ip, listener.port(), 0, index?))
        }
        _ => SocketAddr::new(ip, listener.port()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_addresses_support_both_ip_families_and_optional_ports() {
        for (value, expected) in [
            (" 192.168.1.9 ", "192.168.1.9:57321"),
            ("192.168.1.9:52134", "192.168.1.9:52134"),
            ("2001:db8::1", "[2001:db8::1]:57321"),
            ("[2001:db8::1]:52134", "[2001:db8::1]:52134"),
            ("fe80::1%7", "[fe80::1%7]:57321"),
            ("[fe80::1%7]:52134", "[fe80::1%7]:52134"),
            ("127.0.0.1", "127.0.0.1:57321"),
        ] {
            assert_eq!(
                parse_pairing_address(value).unwrap(),
                expected.parse::<SocketAddr>().unwrap()
            );
        }
        for value in [
            "",
            "hp.local:57321",
            "192.168.1.999",
            "0.0.0.0:57321",
            "[::]:57321",
            "224.0.0.1:57321",
            "[ff02::1]:57321",
            "192.168.1.2:0",
            "192.168.1.2:65536",
        ] {
            assert!(parse_pairing_address(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn advertised_addresses_use_actual_port_and_match_listener_family() {
        let v4 = "0.0.0.0:52134".parse().unwrap();
        assert_eq!(
            interface_address(v4, "192.168.1.2".parse().unwrap(), Some(7)),
            Some("192.168.1.2:52134".parse().unwrap())
        );
        assert!(interface_address(v4, "2001:db8::1".parse().unwrap(), Some(7)).is_none());
        assert!(interface_address(v4, "127.0.0.1".parse().unwrap(), Some(7)).is_none());
        let v6 = "[::]:52134".parse().unwrap();
        assert_eq!(
            interface_address(v6, "fe80::1".parse().unwrap(), Some(7)),
            Some("[fe80::1%7]:52134".parse().unwrap())
        );
        assert!(interface_address(v6, "fe80::1".parse().unwrap(), None).is_none());
        let explicit = "[::1]:52134".parse().unwrap();
        assert_eq!(pairing_addresses(explicit), vec![explicit]);
    }
}
