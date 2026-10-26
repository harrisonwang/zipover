//! Noise supplies the authenticated handshake, forward secrecy, and directional keys.
//! TCP records use implicit increasing nonces; replay, reordering, and tampering fail closed.
use crate::config::PairingKey;
use crate::protocol::FRAME_LIMIT;
use anyhow::{Context, Result, bail};
use snow::{Builder, StatelessTransportState};
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
};

const MAX_RECORD: usize = 65535;
const TAG: usize = 16;
const MAX_PAYLOAD: usize = MAX_RECORD - TAG;

// Never use a short code directly as a Noise PSK: a recorded handshake would
// allow offline guessing. SPAKE2 derives a fresh strong key; Noise confirms it.
pub(crate) fn pairing_key(
    stream: &mut TcpStream,
    initiator: bool,
    code: &str,
    context: &[u8],
) -> Result<PairingKey> {
    use spake2::{Ed25519Group, Identity, Password, Spake2};
    let password = Password::new(code.as_bytes());
    let a = Identity::new(b"zipover numeric pairing v1");
    let b = Identity::new(context);
    let (exchange, outbound) = if initiator {
        Spake2::<Ed25519Group>::start_a(&password, &a, &b)
    } else {
        Spake2::<Ed25519Group>::start_b(&password, &a, &b)
    };
    stream.write_all(&outbound)?;
    let mut inbound = [0; 33];
    stream.read_exact(&mut inbound)?;
    exchange
        .finish(&inbound)
        .map_err(|_| anyhow::anyhow!("配对交换无效"))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("配对密钥长度无效"))
}

pub(crate) struct Cipher {
    state: Arc<StatelessTransportState>,
    nonce: u64,
}

pub(crate) fn handshake(
    stream: &mut TcpStream,
    initiator: bool,
    key: &PairingKey,
    prologue: &[u8],
) -> Result<(Cipher, Cipher)> {
    let builder = Builder::new("Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s".parse()?)
        .psk(0, key)?
        .prologue(prologue)?;
    let mut noise = if initiator {
        builder.build_initiator()?
    } else {
        builder.build_responder()?
    };
    // NNpsk0 has exactly two handshake messages, e + tag (48 bytes) each.
    let mut message = [0; 48];
    while !noise.is_handshake_finished() {
        if noise.is_my_turn() {
            let size = noise.write_message(&[], &mut message)?;
            stream.write_all(&message[..size])?;
        } else {
            stream.read_exact(&mut message)?;
            noise
                .read_message(&message, &mut [])
                .context("认证失败：配对码不匹配或连接被篡改")?;
        }
    }
    let state = Arc::new(noise.into_stateless_transport_mode()?);
    Ok((
        Cipher {
            state: state.clone(),
            nonce: 0,
        },
        Cipher { state, nonce: 0 },
    ))
}

impl Cipher {
    pub fn write_frame(&mut self, stream: &mut impl Write, frame: &[u8]) -> Result<()> {
        if frame.is_empty() || frame.len() > FRAME_LIMIT {
            bail!("发送帧大小无效")
        }
        self.write_record(stream, &(frame.len() as u32).to_be_bytes())?;
        for chunk in frame.chunks(MAX_PAYLOAD) {
            self.write_record(stream, chunk)?;
        }
        Ok(())
    }

    pub fn read_frame(&mut self, stream: &mut impl Read) -> Result<Vec<u8>> {
        let length = self.read_record(stream, 4)?;
        let length = u32::from_be_bytes(length.try_into().expect("已读取四字节长度")) as usize;
        if length == 0 || length > FRAME_LIMIT {
            bail!("接收帧大小无效")
        }
        let mut frame = Vec::with_capacity(length);
        while frame.len() < length {
            frame.extend(self.read_record(stream, (length - frame.len()).min(MAX_PAYLOAD))?);
        }
        Ok(frame)
    }

    fn write_record(&mut self, stream: &mut impl Write, data: &[u8]) -> Result<()> {
        let mut encrypted = vec![0; data.len() + TAG];
        self.state.write_message(self.nonce, data, &mut encrypted)?;
        self.advance()?;
        stream.write_all(&encrypted)?;
        Ok(())
    }

    fn read_record(&mut self, stream: &mut impl Read, length: usize) -> Result<Vec<u8>> {
        let mut encrypted = vec![0; length + TAG];
        stream.read_exact(&mut encrypted)?;
        let mut data = vec![0; length];
        self.state
            .read_message(self.nonce, &encrypted, &mut data)
            .context("消息认证失败（密文损坏、重放或顺序错误）")?;
        self.advance()?;
        Ok(data)
    }

    fn advance(&mut self) -> Result<()> {
        self.nonce = self.nonce.checked_add(1).context("会话 nonce 已用尽")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Cursor, net::TcpListener, thread};

    fn ciphers() -> ((Cipher, Cipher), (Cipher, Cipher)) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            handshake(&mut stream, true, &[5; 32], b"zipover-test").unwrap()
        });
        let (mut stream, _) = listener.accept().unwrap();
        let server = handshake(&mut stream, false, &[5; 32], b"zipover-test").unwrap();
        (client.join().unwrap(), server)
    }

    #[test]
    fn large_frames_round_trip_in_both_directions() {
        let ((mut aw, mut ar), (mut bw, mut br)) = ciphers();
        let payload = vec![0x42; FRAME_LIMIT];
        let mut encrypted = Vec::new();
        aw.write_frame(&mut encrypted, &payload).unwrap();
        assert!(!encrypted.windows(32).any(|w| w == &payload[..32]));
        assert_eq!(br.read_frame(&mut Cursor::new(encrypted)).unwrap(), payload);
        let mut encrypted = Vec::new();
        bw.write_frame(&mut encrypted, b"reply").unwrap();
        assert_eq!(
            ar.read_frame(&mut Cursor::new(encrypted)).unwrap(),
            b"reply"
        );
    }

    #[test]
    fn replay_tamper_and_cross_session_frames_are_rejected() {
        let ((mut writer, _), (_, mut reader)) = ciphers();
        let mut frame = Vec::new();
        writer.write_frame(&mut frame, b"clipboard").unwrap();
        reader.read_frame(&mut Cursor::new(&frame)).unwrap();
        assert!(reader.read_frame(&mut Cursor::new(&frame)).is_err());
        let (_, (_, mut other)) = ciphers();
        assert!(other.read_frame(&mut Cursor::new(&frame)).is_err());
        let ((mut writer, _), (_, mut reader)) = ciphers();
        let mut frame = Vec::new();
        writer.write_frame(&mut frame, b"clipboard").unwrap();
        *frame.last_mut().unwrap() ^= 1;
        assert!(reader.read_frame(&mut Cursor::new(frame)).is_err());
    }

    #[test]
    fn wrong_key_cannot_finish_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            handshake(&mut stream, true, &[5; 32], b"zipover-test").is_err()
        });
        let (mut stream, _) = listener.accept().unwrap();
        assert!(handshake(&mut stream, false, &[6; 32], b"zipover-test").is_err());
        drop(stream);
        assert!(client.join().unwrap());
    }

    fn code_keys(
        code_a: &'static str,
        code_b: &'static str,
        context_b: &'static [u8],
    ) -> (PairingKey, PairingKey) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            pairing_key(&mut stream, true, code_a, b"devices A-B").unwrap()
        });
        let (mut stream, _) = listener.accept().unwrap();
        let key = pairing_key(&mut stream, false, code_b, context_b).unwrap();
        (client.join().unwrap(), key)
    }

    #[test]
    fn numeric_code_uses_fresh_keys_bound_to_both_devices() {
        let (a, b) = code_keys("001234", "001234", b"devices A-B");
        assert_eq!(a, b);
        let (next, _) = code_keys("001234", "001234", b"devices A-B");
        assert_ne!(a, next, "same PIN must not imply same key");
        let (a, b) = code_keys("001234", "001235", b"devices A-B");
        assert_ne!(a, b);
        let (a, b) = code_keys("001234", "001234", b"devices A-C");
        assert_ne!(a, b, "changing device identities must change the key");
    }
}
