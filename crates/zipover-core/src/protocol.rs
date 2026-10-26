use crate::files::TransferEntry;
use serde::{Deserialize, Serialize};

pub(crate) const FRAME_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const CHUNK_SIZE: usize = 1024 * 1024;
pub(crate) const MAX_TRANSFER_BYTES: u64 = 32 * 1024 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_ENTRIES: usize = 100_000;
pub(crate) const PROTOCOL_VERSION: u16 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ClipboardPayload {
    Text(String),
    Image {
        width: u32,
        height: u32,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WireMessage {
    Clipboard {
        id: String,
        payload: ClipboardPayload,
    },
    TransferStart {
        id: String,
        entries: Vec<TransferEntry>,
    },
    TransferChunk {
        id: String,
        file_index: u32,
        offset: u64,
        bytes: Vec<u8>,
    },
    TransferFileEnd {
        id: String,
        file_index: u32,
        sha256: [u8; 32],
    },
    TransferEnd {
        id: String,
    },
    TransferAbort {
        id: String,
    },
    ImageStart {
        id: String,
        fingerprint: u64,
        width: u32,
        height: u32,
        size: u64,
    },
    ImageChunk {
        id: String,
        offset: u64,
        bytes: Vec<u8>,
    },
    ImageEnd {
        id: String,
        sha256: [u8; 32],
    },
}
