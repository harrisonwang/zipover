use super::*;
use if_addrs::{IfAddr, IfOperStatus, Ifv6Addr, Interface};
use std::{
    net::{IpAddr, Ipv6Addr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

fn config() -> DeviceConfig {
    DeviceConfig {
        id: [1; 16],
        name: "test".into(),
        peers: Vec::new(),
    }
}

fn v6_interface(address: &str, index: u32) -> Interface {
    Interface {
        name: format!("test{index}"),
        index: Some(index),
        oper_status: IfOperStatus::Up,
        is_p2p: false,
        addr: IfAddr::V6(Ifv6Addr {
            ip: address.parse().unwrap(),
            netmask: Ipv6Addr::UNSPECIFIED,
            prefixlen: 64,
            broadcast: None,
        }),
        #[cfg(windows)]
        adapter_name: format!("test{index}"),
    }
}

#[test]
fn service_advertises_actual_port_and_only_explicit_addresses() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let service = Discovery::service(&config(), listener.local_addr().unwrap(), false).unwrap();
    assert_eq!(service.get_port(), listener.local_addr().unwrap().port());
    assert_eq!(
        service.get_addresses().iter().copied().collect::<Vec<_>>(),
        vec!["127.0.0.1".parse::<IpAddr>().unwrap()]
    );
    let service =
        Discovery::service(&config(), "[2001:db8::1]:57321".parse().unwrap(), true).unwrap();
    assert_eq!(
        service.get_addresses().iter().copied().collect::<Vec<_>>(),
        vec!["2001:db8::1".parse::<IpAddr>().unwrap()]
    );
    assert!(!service.is_addr_auto());
    let service = Discovery::service(&config(), "[::]:57321".parse().unwrap(), false).unwrap();
    assert!(service.is_addr_auto());
}

#[test]
fn resolved_peers_keep_both_families_and_each_local_link_scope() {
    let mut info = Discovery::service(&config(), "[::]:57321".parse().unwrap(), true)
        .unwrap()
        .as_resolved_service();
    info.addresses = [
        ScopedIp::from("192.168.1.2".parse::<IpAddr>().unwrap()),
        ScopedIp::from(&v6_interface("2001:db8::2", 7)),
        ScopedIp::from(&v6_interface("fe80::2", 7)),
        ScopedIp::from(&v6_interface("fe80::2", 12)),
    ]
    .into_iter()
    .collect();
    let addresses = resolved_addresses(&info);
    assert_eq!(
        addresses,
        vec![
            "192.168.1.2:57321".parse().unwrap(),
            "[2001:db8::2]:57321".parse().unwrap(),
            "[fe80::2%7]:57321".parse().unwrap(),
            "[fe80::2%12]:57321".parse().unwrap(),
        ]
    );
}

#[test]
#[ignore = "requires a working IPv6 multicast interface; run explicitly on a LAN host"]
fn ipv6_multicast_discovers_a_connectable_ipv6_service() {
    let selected = std::env::var("ZIPOVER_TEST_IPV6_INTERFACE").ok();
    let interface = if_addrs::get_if_addrs()
        .unwrap()
        .into_iter()
        .find(|interface| {
            interface.is_oper_up()
                && !interface.is_loopback()
                && !interface.name.starts_with("lo")
                && interface.ip().is_ipv6()
                && selected.as_ref().is_none_or(|name| name == &interface.name)
        })
        .expect("an IPv6 multicast interface");
    let scoped = ScopedIp::from(&interface);
    let ScopedIp::V6(ip) = scoped else {
        unreachable!()
    };
    let listener = crate::runtime::bind_listener(SocketAddr::V6(SocketAddrV6::new(
        *ip.addr(),
        0,
        0,
        ip.scope_id().index,
    )))
    .unwrap();
    let mut config = config();
    config.id = crate::config::random_bytes().unwrap();
    let service = Discovery::service(&config, listener.local_addr().unwrap(), true).unwrap();
    let fullname = service.get_fullname().to_owned();
    let publisher = ServiceDaemon::new().unwrap();
    let browser = ServiceDaemon::new().unwrap();
    // 确保解析不会仅依赖 IPv4 mDNS 而意外通过。
    publisher.disable_interface(IfKind::All).unwrap();
    browser.disable_interface(IfKind::All).unwrap();
    publisher
        .enable_interface(IfKind::IndexV6(interface.index.unwrap()))
        .unwrap();
    browser
        .enable_interface(IfKind::IndexV6(interface.index.unwrap()))
        .unwrap();
    publisher.register(service).unwrap();
    let events = browser.browse(SERVICE_TYPE).unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    let endpoint = loop {
        assert!(
            Instant::now() < deadline,
            "IPv6 mDNS resolve timed out on {}",
            interface.name
        );
        if let Ok(ServiceEvent::ServiceResolved(info)) =
            events.recv_timeout(Duration::from_millis(250))
            && info.get_fullname() == fullname
            && let Some(address) = resolved_addresses(&info)
                .into_iter()
                .find(|address| address.is_ipv6())
        {
            break address;
        }
    };
    let result = TcpStream::connect_timeout(&endpoint, Duration::from_secs(2));
    let _ = publisher.shutdown();
    let _ = browser.shutdown();
    result.unwrap();
}
