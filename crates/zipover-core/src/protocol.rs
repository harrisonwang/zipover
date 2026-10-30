use crate::files::TransferEntry;
use serde::{Deserialize, Serialize};

pub(crate) const FRAME_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const CHUNK_SIZE: usize = 1024 * 1024;
pub(crate) const MAX_TRANSFER_BYTES: u64 = 32 * 1024 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_ENTRIES: usize = 100_000;
pub(crate) const PROTOCOL_VERSION: u16 = 4;

// 按墙上时钟与已知版本生成递增计数，同值时按设备 ID 排序，使各端一致淘汰旧传输。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct ClipboardVersion {
    pub counter: u64,
    pub origin: crate::config::DeviceId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DeliveryStatus {
    Applied,
    Rejected,
    Superseded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WireMessage {
    Clipboard {
        id: String,
        version: ClipboardVersion,
        text: String,
    },
    TransferStart {
        id: String,
        version: ClipboardVersion,
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
        version: ClipboardVersion,
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
    DeliveryResult {
        id: String,
        status: DeliveryStatus,
    },
}
