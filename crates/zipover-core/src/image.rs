use anyhow::{Context, Result, bail};
use arboard::{Clipboard, ImageData};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, io::Cursor, sync::Arc};

use super::transport::PeerHub;
use crate::{
    protocol::{CHUNK_SIZE, WireMessage},
    sync::format_bytes,
};

const MAX_IMAGE_BYTES: usize = 64 * 1024 * 1024;
const MAX_IMAGE_TRANSFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct CapturedImage {
    fingerprint: u64,
    width: u32,
    height: u32,
    bytes: Arc<[u8]>,
}

pub(crate) struct CachedImage {
    raw_fingerprint: u64,
    image: CapturedImage,
}

pub(crate) struct IncomingImage {
    fingerprint: u64,
    width: u32,
    height: u32,
    expected_size: u64,
    bytes: Vec<u8>,
    hasher: Sha256,
}

impl CapturedImage {
    pub(crate) fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    pub(crate) fn summary(&self) -> String {
        crate::i18n::localized!(
            format,
            "image.summary",
            width = self.width,
            height = self.height,
            size = format_bytes(self.bytes.len() as u64)
        )
    }
}

pub(crate) fn capture_image(
    clipboard: &mut Clipboard,
    cache: &mut Option<CachedImage>,
) -> Result<Option<CapturedImage>> {
    let image = match clipboard.get_image() {
        Ok(image) => image,
        Err(arboard::Error::ContentNotAvailable) => {
            *cache = None;
            return Ok(None);
        }
        Err(error) => {
            *cache = None;
            return Err(error).context(crate::i18n::tr("image.read_failed"));
        }
    };

    let width = u32::try_from(image.width).context(crate::i18n::tr("image.too_wide"))?;
    let height = u32::try_from(image.height).context(crate::i18n::tr("image.too_tall"))?;
    let raw_fingerprint = image_fingerprint(width, height, image.bytes.as_ref());
    if let Some(cached) = cache
        && cached.raw_fingerprint == raw_fingerprint
    {
        return Ok(Some(cached.image.clone()));
    }

    let bytes: Arc<[u8]> = encode_png(width, height, image.bytes.as_ref())?
        .into_boxed_slice()
        .into();
    let fingerprint = image_fingerprint(width, height, bytes.as_ref());
    let captured = CapturedImage {
        fingerprint,
        width,
        height,
        bytes,
    };
    *cache = Some(CachedImage {
        raw_fingerprint,
        image: captured.clone(),
    });
    Ok(Some(captured))
}

pub(crate) fn send_image_transfer(
    hub: &PeerHub,
    peers: &[u64],
    id: &str,
    image: &CapturedImage,
) -> Result<()> {
    let result = (|| {
        let size =
            u64::try_from(image.bytes.len()).context(crate::i18n::tr("image.size_overflow"))?;
        if size > MAX_IMAGE_TRANSFER_BYTES as u64 {
            crate::i18n::localized!(
                bail,
                "image.size_limit",
                limit = format_bytes(MAX_IMAGE_TRANSFER_BYTES as u64)
            );
        }
        hub.send_to_targets(
            peers,
            &WireMessage::ImageStart {
                id: id.to_string(),
                fingerprint: image.fingerprint,
                width: image.width,
                height: image.height,
                size,
            },
        )?;

        let mut offset = 0u64;
        for chunk in image.bytes.chunks(CHUNK_SIZE) {
            hub.send_to_targets(
                peers,
                &WireMessage::ImageChunk {
                    id: id.to_string(),
                    offset,
                    bytes: chunk.to_vec(),
                },
            )?;
            offset += chunk.len() as u64;
        }
        let sha256: [u8; 32] = Sha256::digest(image.bytes.as_ref()).into();
        hub.send_to_targets(
            peers,
            &WireMessage::ImageEnd {
                id: id.to_string(),
                sha256,
            },
        )?;
        Ok(())
    })();
    if result.is_err() {
        let _ = hub.send_to_targets(peers, &WireMessage::TransferAbort { id: id.to_string() });
    }
    result
}

impl IncomingImage {
    pub(crate) fn start(
        fingerprint: u64,
        width: u32,
        height: u32,
        expected_size: u64,
    ) -> Result<Self> {
        if width == 0 || height == 0 {
            crate::i18n::localized!(bail, "image.zero_dimensions")
        }
        if rgba_size(width, height)? > MAX_IMAGE_BYTES {
            crate::i18n::localized!(bail, "image.pixel_size_limit")
        }
        if expected_size == 0 || expected_size > MAX_IMAGE_TRANSFER_BYTES as u64 {
            crate::i18n::localized!(bail, "image.transfer_size_limit")
        }
        let capacity = usize::try_from(expected_size)
            .context(crate::i18n::tr("image.size_unrepresentable"))?;
        Ok(Self {
            fingerprint,
            width,
            height,
            expected_size,
            bytes: Vec::with_capacity(capacity),
            hasher: Sha256::new(),
        })
    }

    pub(crate) fn accept_chunk(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() > CHUNK_SIZE {
            crate::i18n::localized!(bail, "image.invalid_chunk_size")
        }
        if offset != self.bytes.len() as u64 {
            crate::i18n::localized!(bail, "image.nonsequential_offset")
        }
        let next = self.bytes.len() as u64 + bytes.len() as u64;
        if next > self.expected_size {
            crate::i18n::localized!(bail, "image.declared_size_exceeded")
        }
        self.bytes.extend_from_slice(bytes);
        self.hasher.update(bytes);
        Ok(())
    }

    pub(crate) fn finish(self, expected_sha256: [u8; 32]) -> Result<(u64, ImageData<'static>)> {
        if self.bytes.len() as u64 != self.expected_size {
            crate::i18n::localized!(
                bail,
                "image.size_mismatch",
                expected = format_bytes(self.expected_size),
                received = format_bytes(self.bytes.len() as u64)
            )
        }
        let actual_sha256: [u8; 32] = self.hasher.finalize().into();
        if actual_sha256 != expected_sha256 {
            crate::i18n::localized!(bail, "image.checksum_mismatch")
        }
        let actual_fingerprint = image_fingerprint(self.width, self.height, &self.bytes);
        if actual_fingerprint != self.fingerprint {
            crate::i18n::localized!(bail, "image.fingerprint_mismatch")
        }
        let (decoded_width, decoded_height, decoded) = decode_png(&self.bytes)?;
        if decoded_width != self.width || decoded_height != self.height {
            crate::i18n::localized!(bail, "image.dimensions_mismatch")
        }
        Ok((
            actual_fingerprint,
            ImageData {
                width: decoded_width as usize,
                height: decoded_height as usize,
                bytes: Cow::Owned(decoded),
            },
        ))
    }
}

pub(crate) fn apply_image(
    clipboard: &mut Clipboard,
    width: u32,
    height: u32,
    bytes: &[u8],
) -> Result<()> {
    let (decoded_width, decoded_height, decoded) = decode_png(bytes)?;
    if decoded_width != width || decoded_height != height {
        crate::i18n::localized!(bail, "image.dimensions_mismatch")
    }
    clipboard
        .set_image(ImageData {
            width: decoded_width as usize,
            height: decoded_height as usize,
            bytes: Cow::Owned(decoded),
        })
        .context(crate::i18n::tr("image.write_failed"))?;
    Ok(())
}

fn rgba_size(width: u32, height: u32) -> Result<usize> {
    usize::try_from(width)
        .ok()
        .and_then(|width| {
            usize::try_from(height)
                .ok()
                .and_then(|height| width.checked_mul(height))
        })
        .and_then(|pixels| pixels.checked_mul(4))
        .context(crate::i18n::tr("image.dimensions_too_large"))
}

fn encode_png(width: u32, height: u32, rgba: &[u8]) -> Result<Vec<u8>> {
    let expected = rgba_size(width, height)?;
    if rgba.len() != expected {
        crate::i18n::localized!(bail, "image.invalid_pixel_size")
    }
    if expected > MAX_IMAGE_BYTES {
        crate::i18n::localized!(bail, "image.pixel_size_limit")
    }

    let mut encoded = Vec::new();
    let mut encoder = png::Encoder::new(&mut encoded, width, height);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder
        .write_header()
        .context(crate::i18n::tr("image.encode_header_failed"))?;
    writer
        .write_image_data(rgba)
        .context(crate::i18n::tr("image.encode_failed"))?;
    drop(writer);
    Ok(encoded)
}

fn decode_png(bytes: &[u8]) -> Result<(u32, u32, Vec<u8>)> {
    let mut decoder = png::Decoder::new(Cursor::new(bytes));
    decoder.set_transformations(png::Transformations::IDENTITY);
    let mut reader = decoder
        .read_info()
        .context(crate::i18n::tr("image.read_header_failed"))?;
    let (decoded_width, decoded_height, color_type, bit_depth) = {
        let info = reader.info();
        (info.width, info.height, info.color_type, info.bit_depth)
    };
    if color_type != png::ColorType::Rgba || bit_depth != png::BitDepth::Eight {
        crate::i18n::localized!(bail, "image.unsupported_format")
    }
    let expected = rgba_size(decoded_width, decoded_height)?;
    if expected > MAX_IMAGE_BYTES || reader.output_buffer_size() > MAX_IMAGE_BYTES {
        crate::i18n::localized!(bail, "image.decoded_size_limit")
    }

    let mut decoded = vec![0u8; reader.output_buffer_size()];
    let output = reader
        .next_frame(&mut decoded)
        .context(crate::i18n::tr("image.decode_failed"))?;
    if output.buffer_size() != expected {
        crate::i18n::localized!(bail, "image.invalid_pixel_size")
    }
    decoded.truncate(output.buffer_size());
    Ok((decoded_width, decoded_height, decoded))
}

fn image_fingerprint(width: u32, height: u32, bytes: &[u8]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(width.to_be_bytes());
    hasher.update(height.to_be_bytes());
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut prefix = [0u8; 8];
    const FINGERPRINT_BYTES: usize = 8;
    prefix.copy_from_slice(&digest[..FINGERPRINT_BYTES]);
    u64::from_be_bytes(prefix)
}

#[cfg(test)]
pub(crate) fn test_image() -> CapturedImage {
    let pixels = test_image_pixels();
    let bytes: Arc<[u8]> = encode_png(640, 480, &pixels).unwrap().into();
    CapturedImage {
        fingerprint: image_fingerprint(640, 480, &bytes),
        width: 640,
        height: 480,
        bytes,
    }
}

#[cfg(test)]
pub(crate) fn test_image_pixels() -> Vec<u8> {
    let mut seed = 0x12345678u32;
    (0..640 * 480 * 4)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FRAME_LIMIT;

    #[test]
    fn png_round_trip_preserves_pixels() {
        let pixels = [
            255, 0, 0, 255, // 红色
            0, 255, 0, 255, // 绿色
        ];
        let encoded = encode_png(2, 1, &pixels).unwrap();
        assert_eq!(&encoded[..8], b"\x89PNG\r\n\x1a\n");
        let (width, height, decoded) = decode_png(&encoded).unwrap();
        assert_eq!((width, height), (2, 1));
        assert_eq!(decoded, pixels);
    }

    #[test]
    fn large_png_uses_chunked_image_transfer() {
        let width = 1_200;
        let height = 1_200;
        let mut pixels = Vec::with_capacity(width * height * 4);
        let mut value = 0x1234_5678u32;
        for _ in 0..(width * height) {
            value = value.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            pixels.extend_from_slice(&value.to_le_bytes());
        }
        let encoded = encode_png(width as u32, height as u32, &pixels).unwrap();
        assert!(encoded.len() > FRAME_LIMIT);

        let fingerprint = image_fingerprint(width as u32, height as u32, &encoded);
        let mut incoming = IncomingImage::start(
            fingerprint,
            width as u32,
            height as u32,
            encoded.len() as u64,
        )
        .unwrap();
        for (offset, chunk) in encoded.chunks(CHUNK_SIZE).enumerate() {
            incoming
                .accept_chunk((offset * CHUNK_SIZE) as u64, chunk)
                .unwrap();
        }
        let sha256: [u8; 32] = Sha256::digest(&encoded).into();
        let (received_fingerprint, image) = incoming.finish(sha256).unwrap();
        assert_eq!(received_fingerprint, fingerprint);
        assert_eq!((image.width, image.height), (width, height));
        assert_eq!(image.bytes.as_ref(), pixels);
    }

    #[test]
    fn incoming_image_rejects_wrong_fingerprint() {
        let pixels = [255, 0, 0, 255];
        let encoded = encode_png(1, 1, &pixels).unwrap();
        let mut incoming = IncomingImage::start(0, 1, 1, encoded.len() as u64).unwrap();
        incoming.accept_chunk(0, &encoded).unwrap();
        let sha256: [u8; 32] = Sha256::digest(&encoded).into();
        assert!(incoming.finish(sha256).is_err());
    }

    #[test]
    fn incoming_image_limits_dimensions_before_allocating_transfer_buffer() {
        assert!(IncomingImage::start(0, 0, 1, 1).is_err());
        assert!(IncomingImage::start(0, u32::MAX, u32::MAX, 1).is_err());
        assert!(IncomingImage::start(0, (MAX_IMAGE_BYTES / 4 + 1) as u32, 1, 1).is_err());
        assert!(IncomingImage::start(0, 1, 1, MAX_IMAGE_TRANSFER_BYTES as u64 + 1).is_err());
    }

    #[test]
    fn incoming_image_checks_png_dimensions_after_authentication() {
        let encoded = encode_png(1, 1, &[1, 2, 3, 255]).unwrap();
        // 发送方能为尺寸不符的 PNG 提供匹配哈希，因此仍须检查 PNG 自身的尺寸。
        let fingerprint = image_fingerprint(2, 1, &encoded);
        let mut incoming = IncomingImage::start(fingerprint, 2, 1, encoded.len() as u64).unwrap();
        incoming.accept_chunk(0, &encoded).unwrap();
        assert!(incoming.finish(Sha256::digest(&encoded).into()).is_err());
    }

    #[test]
    fn incoming_image_rejects_corrupt_png_with_matching_hashes() {
        let encoded = encode_png(1, 1, &[1, 2, 3, 255]).unwrap();
        let truncated = &encoded[..encoded.len() / 2];
        let fingerprint = image_fingerprint(1, 1, truncated);
        let mut incoming = IncomingImage::start(fingerprint, 1, 1, truncated.len() as u64).unwrap();
        incoming.accept_chunk(0, truncated).unwrap();
        assert!(incoming.finish(Sha256::digest(truncated).into()).is_err());
    }
}
