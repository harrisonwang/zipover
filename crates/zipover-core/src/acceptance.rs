//! 由 scripts/verify-lan.py 驱动的真实双主机测试，使用正式的设备发现、加密连接以及文件和图片协议，不改动两端用户的系统剪贴板或正式配对配置。
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
        thread::spawn(move || network_loop(listener, Some(discovery), hub, pairing, tx, options))
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
        println!("PASS {role} pairing: numeric code and persisted trust");
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
                    version: ClipboardVersion::default(),
                    text,
                },
            )?;
            send_file_transfer(
                &hub,
                peers[0],
                &format!("files-{round}"),
                ClipboardVersion::default(),
                &capture_files(std::slice::from_ref(&folder))?,
                &Arc::new(AtomicBool::new(false)),
            )?;
            let image = image::test_image();
            send_image_transfer(
                &hub,
                peers[0],
                "image",
                ClipboardVersion::default(),
                &image,
                &Arc::new(AtomicBool::new(false)),
            )?;
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
                    WireMessage::Clipboard { id, text, .. } => {
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
                    WireMessage::TransferStart { id, entries, .. } => {
                        files = Some(IncomingTransfer::start(
                            id,
                            &format!("test-{}", timestamp_nanos()),
                            entries,
                            &[],
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
                        let (_, pixels) = image.take().unwrap().finish(sha256)?;
                        assert_eq!((pixels.width, pixels.height), (640, 480));
                        assert_eq!(
                            sha2::Sha256::digest(&pixels.bytes),
                            sha2::Sha256::digest(image::test_image_pixels())
                        );
                        got_image = true;
                    }
                    _ => bail!("unexpected fixture message"),
                }
                if got_text && got_files && got_image && !sent_ack {
                    hub.send_to_targets(
                        &peers,
                        &WireMessage::Clipboard {
                            id: "ack".into(),
                            version: ClipboardVersion::default(),
                            text: "ok".into(),
                        },
                    )?;
                    sent_ack = true;
                }
            }
            println!(
                "PASS {role} round {round}: bidirectional text, 2 MiB file, Unicode directory, empty file, PNG"
            );
            // 等待双方队列中的确认消息发出后再断开连接。
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
