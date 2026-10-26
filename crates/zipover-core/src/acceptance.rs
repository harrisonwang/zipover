//! Real two-host test, driven by scripts/verify-lan.py. It uses the production
//! discovery, encrypted connections, and file/image protocol without touching
//! either user's system clipboard or production pairing configuration.
use super::*;
use crate::{
    SyncOptions, config::ConfigStore, discovery::Discovery, image, pairing::Pairing,
    transport::network_loop,
};
use anyhow::{Context, bail};
use std::{
    env, fs,
    net::{SocketAddr, TcpListener},
    sync::mpsc,
};

#[test]
#[ignore = "requires the second host; run scripts/verify-lan.py"]
fn lan_round_trip() {
    let role = env::var("ZIPOVER_TEST_ROLE").expect("ZIPOVER_TEST_ROLE");
    let other = if role == "mac" { "hp" } else { "mac" };
    let options = SyncOptions {
        listen: SocketAddr::from(([0, 0, 0, 0], 45919)),
        connect: env::var("ZIPOVER_TEST_CONNECT").ok(),
    };
    let store = ConfigStore::load().unwrap();
    assert!(store.config.peers.is_empty());
    let listener = TcpListener::bind(options.listen).unwrap();
    listener.set_nonblocking(true).unwrap();
    let discovery = Discovery::start(&store.config, listener.local_addr().unwrap()).unwrap();
    let pairing = Pairing::new(store);
    let hub = PeerHub::default();
    let (tx, rx) = mpsc::sync_channel(32);
    let worker = {
        let hub = hub.clone();
        let pairing = pairing.clone();
        thread::spawn(move || network_loop(listener, discovery, hub, pairing, tx, options))
    };
    let root = env::temp_dir().join(format!("zipover-lan-{}-{}", role, timestamp_nanos()));
    let folder = root.join("目录 with spaces");
    fs::create_dir_all(folder.join("nested")).unwrap();
    let bytes: Vec<u8> = (0..(CHUNK_SIZE * 2 + 17))
        .map(|i| (i % 251) as u8)
        .collect();
    fs::write(folder.join("nested/data.bin"), &bytes).unwrap();
    fs::write(folder.join("empty.txt"), []).unwrap();
    fs::write(folder.join("来源.txt"), &role).unwrap();
    let result = (|| -> Result<()> {
        if role == "mac" {
            let code = pairing.lock().unwrap().invite()?;
            println!("ZIPOVER_TEST_CODE={code}");
        } else {
            let code = match env::var("ZIPOVER_TEST_CODE") {
                Ok(code) => code,
                Err(_) => {
                    let mut code = String::new();
                    std::io::stdin().read_line(&mut code)?;
                    code
                }
            };
            pairing.lock().unwrap().join(&code, None)?;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut state = pairing.lock().unwrap();
            if state.result.is_some() {
                assert_eq!(state.store.config.peers.len(), 1);
                state.cancel();
                break;
            }
            if let Some(error) = &state.error {
                bail!("pairing failed: {error}");
            }
            if Instant::now() >= deadline {
                bail!("mDNS pairing timed out");
            }
            drop(state);
            thread::sleep(Duration::from_millis(100));
        }
        println!("PASS {role} pairing: mDNS, numeric code and persisted trust");
        for round in 0..2 {
            let deadline = Instant::now() + Duration::from_secs(30);
            let peers = loop {
                let peers = hub.active_peer_ids()?;
                if !peers.is_empty() {
                    assert_eq!(peers.len(), 1, "each pair must have one connection");
                    break peers;
                }
                assert!(
                    Instant::now() < deadline,
                    "mDNS did not connect the paired host"
                );
                thread::sleep(Duration::from_millis(100));
            };
            let text = format!("ZIPOVER_LAN_{role}_{round}_中文\nsecond line");
            hub.send_to_targets(
                &peers,
                &WireMessage::Clipboard {
                    id: "text".into(),
                    payload: ClipboardPayload::Text(text),
                },
            )?;
            send_file_transfer(
                &hub,
                &peers,
                &format!("files-{round}"),
                &capture_files(std::slice::from_ref(&folder))?,
            )?;
            let image = image::test_image();
            send_image_transfer(&hub, &peers, "image", image.fingerprint(), &image)?;
            let mut files = None;
            let mut image = None;
            let (mut got_text, mut got_files, mut got_image, mut got_ack, mut sent_ack) =
                (false, false, false, false, false);
            while !(got_text && got_files && got_image && got_ack) {
                let event = rx
                    .recv_timeout(Duration::from_secs(20))
                    .context("waiting for LAN fixture")?;
                let IncomingEvent::Message { message, .. } = event else {
                    bail!("disconnected during fixture")
                };
                match message {
                    WireMessage::Clipboard {
                        id,
                        payload: ClipboardPayload::Text(text),
                    } => {
                        if id == "ack" {
                            got_ack = true;
                        } else {
                            assert_eq!(
                                text,
                                format!("ZIPOVER_LAN_{other}_{round}_中文\nsecond line")
                            );
                            got_text = true;
                        }
                    }
                    WireMessage::TransferStart { id, entries } => {
                        files = Some(IncomingTransfer::start(
                            id,
                            &format!("test-{}", timestamp_nanos()),
                            entries,
                        )?)
                    }
                    WireMessage::TransferChunk {
                        file_index,
                        offset,
                        bytes,
                        ..
                    } => files
                        .as_mut()
                        .unwrap()
                        .accept_chunk(file_index, offset, &bytes)?,
                    WireMessage::TransferFileEnd {
                        file_index, sha256, ..
                    } => files.as_mut().unwrap().finish_file(file_index, sha256)?,
                    WireMessage::TransferEnd { .. } => {
                        let finished = files.take().unwrap().finish()?;
                        let received = &finished.roots[0];
                        assert_eq!(fs::read(received.join("nested/data.bin"))?, bytes);
                        assert_eq!(fs::read(received.join("empty.txt"))?, b"");
                        assert_eq!(fs::read_to_string(received.join("来源.txt"))?, other);
                        finished.cleanup();
                        got_files = true;
                    }
                    WireMessage::ImageStart {
                        fingerprint,
                        width,
                        height,
                        size,
                        ..
                    } => image = Some(IncomingImage::start(fingerprint, width, height, size)?),
                    WireMessage::ImageChunk { offset, bytes, .. } => {
                        image.as_mut().unwrap().accept_chunk(offset, &bytes)?
                    }
                    WireMessage::ImageEnd { sha256, .. } => {
                        let (_, payload) = image.take().unwrap().finish(sha256)?;
                        let ClipboardPayload::Image {
                            width,
                            height,
                            bytes,
                        } = payload
                        else {
                            panic!()
                        };
                        assert_eq!((width, height), (640, 480));
                        assert_eq!(sha2::Sha256::digest(bytes)[..], sha256);
                        got_image = true;
                    }
                    _ => bail!("unexpected fixture message"),
                }
                if got_text && got_files && got_image && !sent_ack {
                    hub.send_to_targets(
                        &peers,
                        &WireMessage::Clipboard {
                            id: "ack".into(),
                            payload: ClipboardPayload::Text("ok".into()),
                        },
                    )?;
                    sent_ack = true;
                }
            }
            println!(
                "PASS {role} round {round}: bidirectional text, 2 MiB file, Unicode directory, empty file, PNG"
            );
            // Wait for both queued acknowledgements to leave before disconnecting.
            thread::sleep(Duration::from_millis(500));
            if round == 0 {
                hub.disconnect_all();
                thread::sleep(Duration::from_millis(500));
                while rx.try_recv().is_ok() {}
            }
        }
        Ok(())
    })();
    hub.shutdown();
    drop(rx);
    worker.join().unwrap();
    fs::remove_dir_all(root).unwrap();
    result.unwrap();
}

use sha2::Digest;
