use super::*;
use crate::{
    config::{ConfigStore, DeviceConfig, random_bytes},
    pairing::Pairing,
    protocol::ClipboardPayload,
};
use std::{fs, path::Path, sync::mpsc::Receiver};

struct Node {
    address: SocketAddr,
    pairing: SharedPairing,
    hub: PeerHub,
    incoming: Receiver<IncomingEvent>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Node {
    fn start(path: &Path, address: Option<SocketAddr>, connect: Option<String>) -> Self {
        let mut store = ConfigStore::open(path.to_owned()).unwrap();
        let listener =
            TcpListener::bind(address.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap())).unwrap();
        let address = listener.local_addr().unwrap();
        store.remember_listen_port(address.port()).unwrap();
        listener.set_nonblocking(true).unwrap();
        let pairing = Pairing::new(store);
        let hub = PeerHub::default();
        let (sender, incoming) = mpsc::sync_channel(32);
        let thread_pairing = pairing.clone();
        let thread_hub = hub.clone();
        let worker = thread::spawn(move || {
            network_loop(
                listener,
                None,
                thread_hub,
                thread_pairing,
                sender,
                SyncOptions {
                    listen: Some(address),
                    connect,
                },
            )
        });
        Self {
            address,
            pairing,
            hub,
            incoming,
            worker: Some(worker),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.hub.shutdown();
        self.worker.take().unwrap().join().unwrap();
    }
}

fn seed(path: &Path, id: DeviceId) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let config = DeviceConfig {
        id,
        name: format!("manual-{}", id[0]),
        peers: Vec::new(),
        listen_port: None,
    };
    fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
}

fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(12);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "manual pairing or reconnection timed out"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn probe(sender: &Node, recipients: &[&Node], text: &str) {
    sender
        .hub
        .send_to_targets(
            &sender.hub.active_peer_ids().unwrap(),
            &WireMessage::Clipboard {
                id: format!("probe-{text}"),
                payload: ClipboardPayload::Text(text.into()),
            },
        )
        .unwrap();
    for receiver in recipients {
        loop {
            match receiver
                .incoming
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
            {
                IncomingEvent::Message {
                    message:
                        WireMessage::Clipboard {
                            payload: ClipboardPayload::Text(value),
                            ..
                        },
                    ..
                } => {
                    assert_eq!(value, text);
                    break;
                }
                IncomingEvent::Disconnected { .. } => {}
                _ => panic!("unexpected manual pairing test message"),
            }
        }
    }
}

#[test]
fn manual_pairing_reconnects_multiple_authenticated_peers_after_restart_without_mdns() {
    let root = std::env::temp_dir().join(format!(
        "zipover-manual-{}",
        hex::encode(random_bytes::<16>().unwrap())
    ));
    let (a_path, b_path, c_path) = (
        root.join("a/devices.json"),
        root.join("b/devices.json"),
        root.join("c/devices.json"),
    );
    // The sender has the greatest ID: saved addresses must not depend on the
    // mDNS rule that only the smaller device initiates a connection.
    seed(&a_path, [3; 16]);
    seed(&b_path, [1; 16]);
    seed(&c_path, [2; 16]);
    let b = Node::start(&b_path, None, None);
    let c = Node::start(&c_path, None, None);
    let a = Node::start(&a_path, None, Some(b.address.to_string()));
    let code = b.pairing.lock().unwrap().invite().unwrap();
    a.pairing.lock().unwrap().join(&code, None).unwrap();
    wait(|| a.pairing.lock().unwrap().store.config.peers.len() == 1);
    let code = c.pairing.lock().unwrap().invite().unwrap();
    // This request must override the process's earlier --connect target.
    a.pairing.lock().unwrap().join_at(&code, c.address).unwrap();
    wait(|| a.pairing.lock().unwrap().store.config.peers.len() == 2);
    wait(|| a.hub.devices().len() == 2 && b.hub.devices().len() == 1 && c.hub.devices().len() == 1);
    {
        let state = a.pairing.lock().unwrap();
        assert_eq!(
            state
                .store
                .config
                .peers
                .iter()
                .find(|peer| peer.id == [1; 16])
                .unwrap()
                .addresses,
            vec![b.address]
        );
        assert_eq!(
            state
                .store
                .config
                .peers
                .iter()
                .find(|peer| peer.id == [2; 16])
                .unwrap()
                .addresses,
            vec![c.address]
        );
    }
    assert!(
        b.pairing.lock().unwrap().store.config.peers[0]
            .addresses
            .is_empty()
    );
    assert!(
        c.pairing.lock().unwrap().store.config.peers[0]
            .addresses
            .is_empty()
    );
    probe(&a, &[&b, &c], "before restart");
    let (a_address, b_address, c_address) = (a.address, b.address, c.address);
    drop((a, b, c));
    // No pairing code, --connect override, discovery service or clipboard.
    let b = Node::start(&b_path, Some(b_address), None);
    let c = Node::start(&c_path, Some(c_address), None);
    let a = Node::start(&a_path, Some(a_address), None);
    wait(|| a.hub.devices().len() == 2 && b.hub.devices().len() == 1 && c.hub.devices().len() == 1);
    probe(&a, &[&b, &c], "after restart");
    probe(&b, &[&a], "reverse direction");
    drop((a, b, c));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn wrong_manual_code_and_cancelled_request_do_not_save_targets() {
    let root = std::env::temp_dir().join(format!(
        "zipover-manual-failed-{}",
        hex::encode(random_bytes::<16>().unwrap())
    ));
    let (a_path, b_path) = (root.join("a/devices.json"), root.join("b/devices.json"));
    seed(&a_path, [3; 16]);
    seed(&b_path, [1; 16]);
    let b = Node::start(&b_path, None, None);
    let a = Node::start(&a_path, None, None);
    let code = b.pairing.lock().unwrap().invite().unwrap();
    let wrong = if code == "000000" { "000001" } else { "000000" };
    a.pairing.lock().unwrap().join_at(wrong, b.address).unwrap();
    wait(|| a.pairing.lock().unwrap().error.is_some());
    assert!(a.pairing.lock().unwrap().store.config.peers.is_empty());
    assert!(b.pairing.lock().unwrap().store.config.peers.is_empty());
    {
        let mut state = a.pairing.lock().unwrap();
        state.join_at(&code, b.address).unwrap();
        state.cancel();
    }
    thread::sleep(Duration::from_millis(250));
    assert!(a.pairing.lock().unwrap().store.config.peers.is_empty());
    let value: DeviceConfig = serde_json::from_slice(&fs::read(&a_path).unwrap()).unwrap();
    assert!(value.peers.is_empty());
    drop((a, b));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_reachable_wrong_device_does_not_block_an_authenticated_fallback_address() {
    let root = std::env::temp_dir().join(format!(
        "zipover-manual-stale-{}",
        hex::encode(random_bytes::<16>().unwrap())
    ));
    let (a_path, b_path, c_path) = (
        root.join("a/devices.json"),
        root.join("b/devices.json"),
        root.join("c/devices.json"),
    );
    seed(&a_path, [3; 16]);
    seed(&b_path, [1; 16]);
    seed(&c_path, [2; 16]);
    let b = Node::start(&b_path, None, None);
    let wrong = Node::start(&c_path, None, None);
    let a = Node::start(&a_path, None, None);
    let code = b.pairing.lock().unwrap().invite().unwrap();
    a.pairing.lock().unwrap().join_at(&code, b.address).unwrap();
    wait(|| a.pairing.lock().unwrap().store.config.peers.len() == 1);
    drop(a);
    wait(|| b.hub.devices().is_empty());
    let pairing = Pairing::new(ConfigStore::open(a_path).unwrap());
    let key = pairing.lock().unwrap().key_for([1; 16]).unwrap();
    let hub = PeerHub::default();
    let (sender, _receiver) = mpsc::sync_channel(32);
    connect_trusted_addresses(
        PeerAddresses::Discovered(vec![wrong.address, b.address]),
        [1; 16],
        key,
        &hub,
        &pairing,
        &sender,
    )
    .unwrap();
    assert_eq!(hub.devices(), vec![[1; 16]]);
    wait(|| b.hub.devices() == vec![[3; 16]]);
    assert!(wrong.hub.devices().is_empty());
    hub.shutdown();
    drop((pairing, b, wrong));
    fs::remove_dir_all(root).unwrap();
}
