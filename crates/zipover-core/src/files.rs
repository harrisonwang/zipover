use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};

use super::transport::PeerHub;
use crate::{
    protocol::{CHUNK_SIZE, MAX_MANIFEST_ENTRIES, MAX_TRANSFER_BYTES, WireMessage},
    sync::format_bytes,
};
const CACHE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const CACHE_DIRECTORY_NAME: &str = "zipover-sync";
const CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum TransferEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub(crate) struct TransferEntry {
    /// 使用路径原始字节，避免 Unix 上非 UTF-8 文件名被替换。
    pub(crate) path: Vec<u8>,
    pub(crate) kind: TransferEntryKind,
    pub(crate) size: u64,
    pub(crate) mode: Option<u32>,
}
pub(crate) struct LocalEntry {
    pub(crate) entry: TransferEntry,
    pub(crate) source: Option<PathBuf>,
    pub(crate) modified_nanos: u128,
}

pub(crate) struct CapturedFiles {
    pub(crate) entries: Vec<LocalEntry>,
    pub(crate) identity: u64,
    pub(crate) internal: bool,
}

pub(crate) fn capture_files(paths: &[PathBuf]) -> Result<CapturedFiles> {
    if paths.is_empty() {
        crate::i18n::localized!(bail, "files.nothing_copied")
    }

    let internal = paths.iter().any(|path| is_internal_cache_path(path));
    let mut entries = Vec::new();
    let mut names = HashSet::new();

    for path in paths {
        let root_name = path
            .file_name()
            .context(crate::i18n::tr("files.name_unavailable"))?
            .to_owned();
        capture_entry(path, PathBuf::from(root_name), &mut entries, &mut names)?;
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // 本地身份必须区分不同来源的同名文件，即使它们保留了相同的元数据。
    for path in paths {
        path.canonicalize()?.hash(&mut hasher);
    }
    for local in &entries {
        local.entry.hash(&mut hasher);
        local.modified_nanos.hash(&mut hasher);
    }

    Ok(CapturedFiles {
        entries,
        identity: hasher.finish(),
        internal,
    })
}

fn capture_entry(
    source: &Path,
    relative: PathBuf,
    entries: &mut Vec<LocalEntry>,
    names: &mut HashSet<Vec<u8>>,
) -> Result<()> {
    if entries.len() >= MAX_MANIFEST_ENTRIES {
        crate::i18n::localized!(bail, "files.too_many_items", limit = MAX_MANIFEST_ENTRIES)
    }

    let metadata = fs::symlink_metadata(source).with_context(|| {
        crate::i18n::localized!(format, "files.read_item_failed", path = source.display())
    })?;
    if metadata.file_type().is_symlink() {
        crate::i18n::localized!(bail, "files.symlink_unsupported", path = source.display())
    }

    let wire_path = portable_relative_path(&relative)?;
    if !names.insert(path_collision_key(&wire_path)) {
        crate::i18n::localized!(
            bail,
            "files.duplicate_copied_path",
            path = path_display(&wire_path)
        )
    }
    let normalized_path = safe_relative_path(&wire_path)?;

    let (kind, size, child_source) = if metadata.is_dir() {
        (TransferEntryKind::Directory, 0, None)
    } else if metadata.is_file() {
        (
            TransferEntryKind::File,
            metadata.len(),
            Some(source.to_path_buf()),
        )
    } else {
        crate::i18n::localized!(
            bail,
            "files.special_file_unsupported",
            path = source.display()
        )
    };

    entries.push(LocalEntry {
        entry: TransferEntry {
            path: wire_path,
            kind,
            size,
            mode: file_mode(&metadata),
        },
        source: child_source,
        modified_nanos: modified_nanos(&metadata),
    });

    if matches!(kind, TransferEntryKind::Directory) {
        let mut children = fs::read_dir(source)
            .with_context(|| {
                crate::i18n::localized!(
                    format,
                    "files.read_directory_failed",
                    path = source.display()
                )
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| {
                crate::i18n::localized!(
                    format,
                    "files.read_directory_contents_failed",
                    path = source.display()
                )
            })?;
        children.sort_by_key(|child| child.file_name());
        for child in children {
            capture_entry(
                &child.path(),
                normalized_path.join(child.file_name()),
                entries,
                names,
            )?;
        }
    }

    Ok(())
}

fn is_internal_cache_path(path: &Path) -> bool {
    // 旧缓存仍可能被系统剪贴板引用，也应阻止回传。
    [
        cache_directory(),
        env::temp_dir().join(CACHE_DIRECTORY_NAME),
    ]
    .iter()
    .any(|root| {
        path.starts_with(root)
            || path
                .canonicalize()
                .ok()
                .zip(root.canonicalize().ok())
                .is_some_and(|(path, root)| path.starts_with(root))
    })
}

fn manifest_identity(
    directory: &Path,
    roots: &[PathBuf],
    entries: &[TransferEntry],
) -> Result<u64> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for root in roots {
        root.canonicalize()?.hash(&mut hasher);
    }
    for entry in entries {
        entry.hash(&mut hasher);
        let path = directory.join(safe_relative_path(&entry.path)?);
        let metadata = fs::symlink_metadata(&path).with_context(|| {
            crate::i18n::localized!(
                format,
                "files.read_received_metadata_failed",
                path = path.display()
            )
        })?;
        modified_nanos(&metadata).hash(&mut hasher);
    }
    Ok(hasher.finish())
}

pub(crate) fn send_file_transfer(
    hub: &PeerHub,
    peers: &[u64],
    id: &str,
    files: &CapturedFiles,
) -> Result<()> {
    let result = send_file_transfer_inner(hub, peers, id, files);
    if result.is_err() {
        let _ = hub.send_to_targets(peers, &WireMessage::TransferAbort { id: id.to_string() });
    }
    result
}

fn send_file_transfer_inner(
    hub: &PeerHub,
    peers: &[u64],
    id: &str,
    files: &CapturedFiles,
) -> Result<()> {
    let entries = files
        .entries
        .iter()
        .map(|local| local.entry.clone())
        .collect::<Vec<_>>();
    let total_bytes = entries.iter().try_fold(0u64, |total, entry| {
        total
            .checked_add(entry.size)
            .context(crate::i18n::tr("files.total_size_overflow"))
    })?;
    if total_bytes > MAX_TRANSFER_BYTES {
        crate::i18n::localized!(
            bail,
            "files.total_size_limit",
            limit = MAX_TRANSFER_BYTES / (1024 * 1024 * 1024)
        )
    }

    hub.send_to_targets(
        peers,
        &WireMessage::TransferStart {
            id: id.to_string(),
            entries,
        },
    )?;

    let mut file_index = 0u32;
    let mut buffer = vec![0u8; CHUNK_SIZE];
    for local in &files.entries {
        if !matches!(local.entry.kind, TransferEntryKind::File) {
            continue;
        }

        let source = local
            .source
            .as_ref()
            .context(crate::i18n::tr("files.source_path_missing"))?;
        crate::i18n::localized!(
            eprintln,
            "files.reading",
            path = path_display(&local.entry.path)
        );
        let before = fs::symlink_metadata(source).with_context(|| {
            crate::i18n::localized!(
                format,
                "files.read_metadata_failed",
                path = source.display()
            )
        })?;
        if !before.is_file()
            || before.len() != local.entry.size
            || file_mode(&before) != local.entry.mode
            || modified_nanos(&before) != local.modified_nanos
        {
            crate::i18n::localized!(
                bail,
                "files.changed_before_transfer",
                path = source.display()
            )
        }
        let mut file = File::open(source).with_context(|| {
            crate::i18n::localized!(format, "files.open_failed", path = source.display())
        })?;
        let mut hasher = Sha256::new();
        let mut offset = 0u64;

        loop {
            let length = file.read(&mut buffer).with_context(|| {
                crate::i18n::localized!(format, "files.read_failed", path = source.display())
            })?;
            if length == 0 {
                break;
            }

            let next_offset = offset + length as u64;
            if next_offset > local.entry.size {
                crate::i18n::localized!(bail, "files.grew_during_transfer", path = source.display())
            }
            hasher.update(&buffer[..length]);
            hub.send_to_targets(
                peers,
                &WireMessage::TransferChunk {
                    id: id.to_string(),
                    file_index,
                    offset,
                    bytes: buffer[..length].to_vec(),
                },
            )?;
            offset = next_offset;
        }

        if offset != local.entry.size {
            crate::i18n::localized!(
                bail,
                "files.changed_during_transfer",
                path = source.display()
            )
        }
        let after = fs::symlink_metadata(source).with_context(|| {
            crate::i18n::localized!(
                format,
                "files.read_metadata_failed",
                path = source.display()
            )
        })?;
        if !after.is_file()
            || after.len() != local.entry.size
            || file_mode(&after) != local.entry.mode
            || modified_nanos(&after) != local.modified_nanos
        {
            crate::i18n::localized!(
                bail,
                "files.changed_during_transfer",
                path = source.display()
            )
        }
        let sha256: [u8; 32] = hasher.finalize().into();
        hub.send_to_targets(
            peers,
            &WireMessage::TransferFileEnd {
                id: id.to_string(),
                file_index,
                sha256,
            },
        )?;
        file_index = file_index
            .checked_add(1)
            .context(crate::i18n::tr("files.count_overflow"))?;
    }

    hub.send_to_targets(peers, &WireMessage::TransferEnd { id: id.to_string() })
}

pub(crate) struct IncomingTransfer {
    directory: PathBuf,
    parts_directory: PathBuf,
    entries: Vec<TransferEntry>,
    files: Vec<IncomingFile>,
    pub(crate) roots: Vec<PathBuf>,
    committed: bool,
}

struct IncomingFile {
    final_path: PathBuf,
    temporary_path: PathBuf,
    expected_size: u64,
    received_size: u64,
    hasher: Sha256,
    file: Option<File>,
    finished: bool,
}

pub(crate) struct FinishedTransfer {
    pub(crate) fingerprint: u64,
    pub(crate) roots: Vec<PathBuf>,
    directory: PathBuf,
}

impl IncomingTransfer {
    pub(crate) fn start(
        id: String,
        cache_scope: &str,
        entries: Vec<TransferEntry>,
    ) -> Result<Self> {
        if entries.is_empty() {
            crate::i18n::localized!(bail, "files.empty_manifest")
        }
        if entries.len() > MAX_MANIFEST_ENTRIES {
            crate::i18n::localized!(bail, "files.manifest_item_limit")
        }
        let total_bytes = entries.iter().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.size)
                .context(crate::i18n::tr("files.manifest_size_overflow"))
        })?;
        if total_bytes > MAX_TRANSFER_BYTES {
            crate::i18n::localized!(bail, "files.transfer_size_limit")
        }

        let cache = cache_directory();
        ensure_private_directory(&cache)?;
        // 随机后缀加独占创建，避免重用远端提供的消息 ID 所对应的旧目录。
        let directory = cache.join(format!(
            "{}-{}",
            transfer_component(&format!("{cache_scope}-{id}")),
            hex::encode(crate::config::random_bytes::<16>()?)
        ));
        create_private_directory(&directory)?;
        let parts_directory = directory.join(".parts");
        let result = (|| {
            create_private_directory(&parts_directory).with_context(|| {
                crate::i18n::localized!(
                    format,
                    "files.create_transfer_directory_failed",
                    path = directory.display()
                )
            })?;

            let mut size_file = create_private_file(&directory.join(".size"))?;
            write!(size_file, "{total_bytes}")?;
            Self::start_inner(directory.clone(), parts_directory, entries)
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&directory);
        }
        result
    }

    pub(crate) fn cache_directory(&self) -> &Path {
        &self.directory
    }

    fn start_inner(
        directory: PathBuf,
        parts_directory: PathBuf,
        entries: Vec<TransferEntry>,
    ) -> Result<Self> {
        let mut normalized_paths = Vec::with_capacity(entries.len());
        let mut path_kinds = HashMap::with_capacity(entries.len());
        for entry in &entries {
            let relative = safe_relative_path(&entry.path)?;
            let key = normalized_collision_key(&relative)?;
            if path_kinds.insert(key, entry.kind).is_some() {
                crate::i18n::localized!(
                    bail,
                    "files.duplicate_manifest_path",
                    path = path_display(&entry.path)
                )
            }
            normalized_paths.push(relative);
        }

        // 先检查完整路径树，避免“文件 foo”和“目录 foo/bar”在创建或重命名时产生歧义。
        for (entry, relative) in entries.iter().zip(&normalized_paths) {
            let mut ancestor = relative.parent();
            while let Some(path) = ancestor {
                if path.as_os_str().is_empty() {
                    break;
                }
                let key = normalized_collision_key(path)?;
                if path_kinds.get(&key) == Some(&TransferEntryKind::File) {
                    crate::i18n::localized!(
                        bail,
                        "files.file_used_as_directory",
                        path = path_display(&entry.path)
                    )
                }
                ancestor = path.parent();
            }
        }

        let mut roots = Vec::new();
        let mut files = Vec::new();
        let contents_directory = directory.join("contents");

        for (entry_index, (entry, relative)) in entries.iter().zip(&normalized_paths).enumerate() {
            let final_path = contents_directory.join(relative);
            if relative.components().count() == 1 {
                roots.push(final_path.clone());
            }

            match entry.kind {
                TransferEntryKind::Directory => {
                    if entry.size != 0 {
                        crate::i18n::localized!(
                            bail,
                            "files.nonzero_directory_size",
                            path = path_display(&entry.path)
                        )
                    }
                    fs::create_dir_all(&final_path).with_context(|| {
                        crate::i18n::localized!(
                            format,
                            "files.create_directory_failed",
                            path = final_path.display()
                        )
                    })?;
                }
                TransferEntryKind::File => {
                    let parent = final_path
                        .parent()
                        .context(crate::i18n::tr("files.parent_missing"))?;
                    fs::create_dir_all(parent).with_context(|| {
                        crate::i18n::localized!(
                            format,
                            "files.create_parent_failed",
                            path = parent.display()
                        )
                    })?;
                    let temporary_path = parts_directory.join(format!("{entry_index}.part"));
                    files.push(IncomingFile {
                        final_path,
                        temporary_path,
                        expected_size: entry.size,
                        received_size: 0,
                        hasher: Sha256::new(),
                        file: None,
                        finished: false,
                    });
                }
            }
        }

        if roots.is_empty() {
            crate::i18n::localized!(bail, "files.manifest_roots_missing")
        }

        Ok(Self {
            directory,
            parts_directory,
            entries,
            files,
            roots,
            committed: false,
        })
    }

    pub(crate) fn accept_chunk(
        &mut self,
        file_index: u32,
        offset: u64,
        bytes: &[u8],
    ) -> Result<()> {
        if bytes.is_empty() || bytes.len() > CHUNK_SIZE {
            crate::i18n::localized!(bail, "files.invalid_chunk_size")
        }
        let file = self
            .files
            .get_mut(file_index as usize)
            .context(crate::i18n::tr("files.invalid_chunk_index"))?;
        if file.finished {
            crate::i18n::localized!(bail, "files.chunk_after_completion")
        }
        if offset != file.received_size {
            crate::i18n::localized!(
                bail,
                "files.nonsequential_offset",
                expected = file.received_size,
                received = offset
            )
        }
        let next_size = file.received_size + bytes.len() as u64;
        if next_size > file.expected_size {
            crate::i18n::localized!(bail, "files.declared_size_exceeded")
        }

        if file.file.is_none() {
            file.file = Some(create_private_file(&file.temporary_path).with_context(|| {
                crate::i18n::localized!(
                    format,
                    "files.create_temporary_failed",
                    path = file.temporary_path.display()
                )
            })?);
        }
        let handle = file
            .file
            .as_mut()
            .context(crate::i18n::tr("files.handle_closed"))?;
        handle
            .write_all(bytes)
            .context(crate::i18n::tr("files.write_chunk_failed"))?;
        file.hasher.update(bytes);
        file.received_size = next_size;
        Ok(())
    }

    pub(crate) fn finish_file(&mut self, file_index: u32, expected_sha256: [u8; 32]) -> Result<()> {
        let file = self
            .files
            .get_mut(file_index as usize)
            .context(crate::i18n::tr("files.invalid_completion_index"))?;
        if file.finished {
            crate::i18n::localized!(bail, "files.already_complete")
        }
        if file.received_size != file.expected_size {
            crate::i18n::localized!(
                bail,
                "files.size_mismatch",
                expected = file.expected_size,
                received = file.received_size
            )
        }

        let actual_sha256: [u8; 32] = file.hasher.clone().finalize().into();
        if actual_sha256 != expected_sha256 {
            crate::i18n::localized!(bail, "files.checksum_mismatch")
        }

        let mut handle = match file.file.take() {
            Some(handle) => handle,
            None => create_private_file(&file.temporary_path).with_context(|| {
                crate::i18n::localized!(
                    format,
                    "files.create_empty_failed",
                    path = file.temporary_path.display()
                )
            })?,
        };
        handle
            .flush()
            .context(crate::i18n::tr("files.flush_failed"))?;
        drop(handle);
        fs::hard_link(&file.temporary_path, &file.final_path).with_context(|| {
            crate::i18n::localized!(
                format,
                "files.commit_failed",
                path = file.final_path.display()
            )
        })?;
        fs::remove_file(&file.temporary_path).with_context(|| {
            crate::i18n::localized!(
                format,
                "files.remove_temporary_failed",
                path = file.temporary_path.display()
            )
        })?;
        file.finished = true;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<FinishedTransfer> {
        if self.files.iter().any(|file| !file.finished) {
            crate::i18n::localized!(bail, "files.verification_incomplete")
        }
        // 先读取元数据计算指纹，再恢复目录权限，避免无权进入目录时无法完成收尾。
        let contents_directory = self.directory.join("contents");
        let local_fingerprint = manifest_identity(&contents_directory, &self.roots, &self.entries)?;
        for entry in self.entries.iter().rev() {
            let path = contents_directory.join(safe_relative_path(&entry.path)?);
            apply_mode(&path, entry.mode)?;
        }
        if self.parts_directory.exists() {
            fs::remove_dir_all(&self.parts_directory).with_context(|| {
                crate::i18n::localized!(
                    format,
                    "files.remove_chunks_failed",
                    path = self.parts_directory.display()
                )
            })?;
        }
        // 完成时间与容量信息保留在私有父目录中，不受源文件权限影响。
        create_private_file(&self.directory.join(".complete"))?;
        self.committed = true;
        Ok(FinishedTransfer {
            fingerprint: local_fingerprint,
            roots: std::mem::take(&mut self.roots),
            directory: std::mem::take(&mut self.directory),
        })
    }

    pub(crate) fn summary(&self) -> String {
        entry_summary(self.entries.iter())
    }
}

impl Drop for IncomingTransfer {
    fn drop(&mut self) {
        if !self.committed {
            self.files.clear();
            let _ = remove_cached_directory(&self.directory);
        }
    }
}

impl FinishedTransfer {
    pub(crate) fn cleanup(self) {
        let _ = remove_cached_directory(&self.directory);
    }
}
pub(crate) fn files_summary(files: &CapturedFiles) -> String {
    entry_summary(files.entries.iter().map(|local| &local.entry))
}

fn entry_summary<'a>(entries: impl Iterator<Item = &'a TransferEntry>) -> String {
    let (mut file_count, mut directory_count, mut total_bytes) = (0usize, 0usize, 0u64);
    for entry in entries {
        match entry.kind {
            TransferEntryKind::File => file_count += 1,
            TransferEntryKind::Directory => directory_count += 1,
        }
        total_bytes += entry.size;
    }
    crate::i18n::localized!(
        format,
        "files.summary",
        files = file_count,
        folders = directory_count,
        size = format_bytes(total_bytes)
    )
}

pub(crate) fn portable_relative_path(path: &Path) -> Result<Vec<u8>> {
    let mut components = Vec::new();
    for component in path.components() {
        let component = os_str_bytes(component.as_os_str());
        if component.is_empty() || component == b"." || component == b".." {
            crate::i18n::localized!(bail, "files.invalid_copied_component")
        }
        components.push(portable_component(&component));
    }
    if components.is_empty() {
        crate::i18n::localized!(bail, "files.empty_copied_path")
    }
    Ok(components.join(&b'/'))
}

pub(crate) fn safe_relative_path(path: &[u8]) -> Result<PathBuf> {
    let mut result = PathBuf::new();
    for component in path.split(|byte| matches!(*byte, b'/' | b'\\')) {
        if component.is_empty() || component == b"." || component == b".." {
            crate::i18n::localized!(
                bail,
                "files.invalid_transfer_component",
                path = path_display(path)
            )
        }
        result.push(os_string_from_bytes(&portable_component(component))?);
    }
    Ok(result)
}

pub(crate) fn path_collision_key(path: &[u8]) -> Vec<u8> {
    if let Ok(text) = std::str::from_utf8(path) {
        return text.replace('\\', "/").to_lowercase().into_bytes();
    }
    path.iter()
        .map(|byte| match byte {
            b'\\' => b'/',
            b'A'..=b'Z' => byte.to_ascii_lowercase(),
            _ => *byte,
        })
        .collect()
}

fn normalized_collision_key(path: &Path) -> Result<Vec<u8>> {
    Ok(path_collision_key(&portable_relative_path(path)?))
}

fn portable_component(value: &[u8]) -> Vec<u8> {
    let mut result: Vec<u8> = value
        .iter()
        .map(|byte| {
            if *byte < 0x20
                || matches!(
                    *byte,
                    b'<' | b'>' | b':' | b'"' | b'/' | b'\\' | b'|' | b'?' | b'*'
                )
            {
                b'_'
            } else {
                *byte
            }
        })
        .collect();
    while result
        .last()
        .is_some_and(|byte| *byte == b'.' || *byte == b' ')
    {
        result.pop();
    }
    if result.is_empty() {
        result.push(b'_');
    }
    let uppercase = result
        .iter()
        .map(|byte| byte.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let device_name_end = uppercase
        .iter()
        .position(|byte| *byte == b'.')
        .unwrap_or(uppercase.len());
    let reserved = matches!(
        &uppercase[..device_name_end],
        b"CON"
            | b"PRN"
            | b"AUX"
            | b"NUL"
            | [b'C', b'O', b'M', b'1'..=b'9']
            | [b'L', b'P', b'T', b'1'..=b'9']
    );
    if reserved {
        result.insert(device_name_end, b'_');
    }
    result
}

fn path_display(path: &[u8]) -> String {
    String::from_utf8_lossy(path).into_owned()
}

fn os_str_bytes(value: &OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        value.as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        value.to_string_lossy().as_bytes().to_vec()
    }
}

fn os_string_from_bytes(value: &[u8]) -> Result<OsString> {
    #[cfg(unix)]
    {
        Ok(OsString::from_vec(value.to_vec()))
    }
    #[cfg(not(unix))]
    {
        String::from_utf8(value.to_vec())
            .map(OsString::from)
            .context(crate::i18n::tr("files.non_utf8_windows_name"))
    }
}

fn safe_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn transfer_component(value: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    let mut prefix = safe_component(value);
    prefix.truncate(prefix.len().min(64));
    format!("{prefix}-{:016x}", hasher.finish())
}

fn cache_directory() -> PathBuf {
    #[cfg(unix)]
    let name = format!(
        "{CACHE_DIRECTORY_NAME}-{}",
        rustix::process::geteuid().as_raw()
    );
    #[cfg(not(unix))]
    let name = CACHE_DIRECTORY_NAME;
    env::temp_dir().join(name)
}

fn create_private_directory(path: &Path) -> Result<()> {
    let builder = &mut fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(path).with_context(|| {
        crate::i18n::localized!(
            format,
            "files.private_cache_required",
            path = path.display()
        )
    })
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match create_private_directory(path) {
        Ok(()) => {}
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind)
                == Some(std::io::ErrorKind::AlreadyExists) => {}
        Err(error) => return Err(error),
    }
    validate_private_directory(path)
}

fn validate_private_directory(path: &Path) -> Result<()> {
    // symlink_metadata 不跟随预先放置的链接，也不修复其他用户创建的目录。
    let metadata = fs::symlink_metadata(path)?;
    let private = metadata.is_dir();
    #[cfg(unix)]
    let private = private
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.permissions().mode() & 0o077 == 0;
    if !private {
        crate::i18n::localized!(bail, "files.private_cache_required", path = path.display());
    }
    Ok(())
}

fn create_private_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path)
}

pub(crate) fn cleanup_cache(protected: &[PathBuf]) {
    let directory = cache_directory();
    if directory.try_exists().unwrap_or(false) {
        let _ = maintain_cache(&directory, protected, 0, CACHE_MAX_BYTES, SystemTime::now());
    }
}

pub(crate) fn reserve_cache_space(protected: &[PathBuf], bytes: u64) -> Result<()> {
    let directory = cache_directory();
    ensure_private_directory(&directory)?;
    maintain_cache(
        &directory,
        protected,
        bytes,
        CACHE_MAX_BYTES,
        SystemTime::now(),
    )
}

fn maintain_cache(
    directory: &Path,
    protected: &[PathBuf],
    incoming_bytes: u64,
    limit: u64,
    now: SystemTime,
) -> Result<()> {
    validate_private_directory(directory)?;
    let protected = protected
        .iter()
        .map(|path| path.canonicalize().unwrap_or_else(|_| path.clone()))
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();
    let mut total = u128::from(incoming_bytes);
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_dir() {
            continue;
        }
        let path = entry.path().canonicalize()?;
        let is_protected = protected
            .iter()
            .any(|keep| keep.starts_with(&path) || path.starts_with(keep));
        let completed = fs::symlink_metadata(path.join(".complete"))
            .ok()
            .filter(|metadata| metadata.is_file());
        let modified = completed.as_ref().unwrap_or(&metadata).modified()?;
        let expired = now
            .duration_since(modified)
            .is_ok_and(|age| age >= CACHE_MAX_AGE);
        if !is_protected && expired && remove_cached_directory(&path).is_ok() {
            continue;
        }
        // .size 存储声明的总大小，也为尚未完成的传输预留空间。
        // 损坏的记录按超过配额处理，不能将未知占用误算为零。
        let size = fs::symlink_metadata(path.join(".size"))
            .ok()
            .filter(|metadata| metadata.is_file() && metadata.len() <= 20)
            .and_then(|_| fs::read_to_string(path.join(".size")).ok())
            .and_then(|size| size.parse::<u64>().ok())
            .unwrap_or_else(|| limit.saturating_add(1));
        total += u128::from(size);
        if !is_protected && completed.is_some() {
            candidates.push((modified, path, size));
        }
    }
    candidates.sort_by_key(|(modified, _, _)| *modified);
    for (_, path, size) in candidates {
        if total <= u128::from(limit) {
            break;
        }
        if remove_cached_directory(&path).is_ok() {
            total -= u128::from(size);
        }
    }
    if total > u128::from(limit) {
        crate::i18n::localized!(bail, "files.cache_size_limit", limit = format_bytes(limit));
    }
    Ok(())
}

fn remove_cached_directory(path: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => return Ok(()),
        #[cfg(unix)]
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
        Err(error) => return Err(error),
    }
    // 源目录可能为只读或不可遍历；删除缓存时恢复目录的用户权限，不跟随链接。
    #[cfg(unix)]
    fn unlock_directories(path: &Path) -> std::io::Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            fs::set_permissions(path, fs::Permissions::from_mode(metadata.mode() | 0o700))?;
            for entry in fs::read_dir(path)? {
                unlock_directories(&entry?.path())?;
            }
        }
        Ok(())
    }
    #[cfg(unix)]
    {
        unlock_directories(path)?;
        fs::remove_dir_all(path)
    }
}

fn modified_nanos(metadata: &fs::Metadata) -> u128 {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

#[cfg(unix)]
pub(crate) fn file_mode(metadata: &fs::Metadata) -> Option<u32> {
    Some(metadata.permissions().mode() & 0o7777)
}

#[cfg(not(unix))]
pub(crate) fn file_mode(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn apply_mode(path: &Path, mode: Option<u32>) -> Result<()> {
    if let Some(mode) = mode {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).with_context(|| {
            crate::i18n::localized!(
                format,
                "files.restore_permissions_failed",
                path = path.display()
            )
        })?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn apply_mode(_path: &Path, _mode: Option<u32>) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "zipover-cache-test-{}",
                hex::encode(crate::config::random_bytes::<16>().unwrap())
            ));
            create_private_directory(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = remove_cached_directory(&self.0);
        }
    }

    fn cached_transfer(
        root: &Path,
        name: &str,
        size: u64,
        completed_at: Option<SystemTime>,
    ) -> PathBuf {
        let path = root.join(name);
        create_private_directory(&path).unwrap();
        write!(create_private_file(&path.join(".size")).unwrap(), "{size}").unwrap();
        if let Some(completed_at) = completed_at {
            create_private_file(&path.join(".complete"))
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(completed_at))
                .unwrap();
        }
        path
    }

    #[test]
    fn distinct_sources_with_identical_file_metadata_have_distinct_identities() {
        let root = TestDirectory::new();
        let modified = SystemTime::now() - Duration::from_secs(60);
        let paths = [root.0.join("a"), root.0.join("b")].map(|directory| {
            fs::create_dir(&directory).unwrap();
            directory.join("same.txt")
        });
        for (path, contents) in paths.iter().zip([b"first", b"other"]) {
            let mut file = File::create(path).unwrap();
            file.write_all(contents).unwrap();
            file.set_times(std::fs::FileTimes::new().set_modified(modified))
                .unwrap();
        }
        let first = capture_files(&paths[..1]).unwrap().identity;
        let other = capture_files(&paths[1..]).unwrap().identity;
        assert_ne!(first, other);
        assert_eq!(first, capture_files(&paths[..1]).unwrap().identity);
    }

    #[test]
    fn repeated_transfer_ids_do_not_reuse_cache_directories() {
        let entry = TransferEntry {
            path: b"empty".to_vec(),
            kind: TransferEntryKind::File,
            size: 0,
            mode: None,
        };
        let first =
            IncomingTransfer::start("repeated".into(), "test", vec![entry.clone()]).unwrap();
        let second = IncomingTransfer::start("repeated".into(), "test", vec![entry]).unwrap();
        assert_ne!(first.directory, second.directory);
        assert!(first.directory.is_dir());
        assert!(second.directory.is_dir());
    }

    #[test]
    fn private_cache_refuses_existing_files() {
        let root = TestDirectory::new();
        let path = root.0.join("file");
        fs::write(&path, b"preserve").unwrap();
        assert!(ensure_private_directory(&path).is_err());
        assert_eq!(fs::read(path).unwrap(), b"preserve");
        ensure_private_directory(&root.0).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn private_cache_refuses_symlinks_and_shared_permissions() {
        use std::os::unix::fs::symlink;

        let root = TestDirectory::new();
        let shared = root.0.join("shared");
        create_private_directory(&shared).unwrap();
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(ensure_private_directory(&shared).is_err());
        assert_eq!(fs::metadata(&shared).unwrap().mode() & 0o777, 0o755);

        let link = root.0.join("link");
        symlink(&root.0, &link).unwrap();
        assert!(ensure_private_directory(&link).is_err());
        assert!(fs::symlink_metadata(link).unwrap().file_type().is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn receiving_files_stays_private_before_and_after_restoring_source_permissions() {
        let entry = TransferEntry {
            path: b"sample.txt".to_vec(),
            kind: TransferEntryKind::File,
            size: 7,
            mode: Some(0o644),
        };
        let mut incoming = IncomingTransfer::start("private".into(), "test", vec![entry]).unwrap();
        let directory = incoming.directory.clone();
        for path in [
            cache_directory(),
            directory.clone(),
            incoming.parts_directory.clone(),
        ] {
            assert_eq!(fs::metadata(path).unwrap().mode() & 0o777, 0o700);
        }
        incoming.accept_chunk(0, 0, b"payload").unwrap();
        assert_eq!(
            fs::metadata(&incoming.files[0].temporary_path)
                .unwrap()
                .mode()
                & 0o777,
            0o600
        );
        incoming
            .finish_file(0, Sha256::digest(b"payload").into())
            .unwrap();
        assert_eq!(
            fs::metadata(&incoming.roots[0]).unwrap().mode() & 0o777,
            0o600
        );
        let finished = incoming.finish().unwrap();
        assert_eq!(
            fs::metadata(&finished.roots[0]).unwrap().mode() & 0o777,
            0o644
        );
        assert_eq!(fs::metadata(&directory).unwrap().mode() & 0o777, 0o700);
        assert_eq!(
            finished.fingerprint,
            capture_files(&finished.roots).unwrap().identity
        );
        assert!(capture_files(&finished.roots).unwrap().internal);
        assert_eq!(fs::read_to_string(directory.join(".size")).unwrap(), "7");
        assert!(directory.join(".complete").is_file());
        finished.cleanup();
        assert!(!directory.exists());
    }

    #[test]
    fn cache_quota_evicts_oldest_completed_transfer_and_reserves_incoming_space() {
        let root = TestDirectory::new();
        let now = SystemTime::now();
        let older = cached_transfer(&root.0, "older", 6, Some(now - Duration::from_secs(2)));
        let newer = cached_transfer(&root.0, "newer", 6, Some(now));
        let active = cached_transfer(&root.0, "active", 2, None);
        maintain_cache(&root.0, std::slice::from_ref(&active), 4, 12, now).unwrap();
        assert!(!older.exists());
        assert!(newer.exists());
        assert!(active.exists());
    }

    #[test]
    fn cache_quota_rejects_new_transfer_without_deleting_clipboard_or_active_files() {
        let root = TestDirectory::new();
        let now = SystemTime::now();
        let clipboard = cached_transfer(&root.0, "clipboard", 7, Some(now));
        let copied_file = clipboard.join("file.txt");
        fs::write(&copied_file, b"keep").unwrap();
        let active = cached_transfer(&root.0, "active", 3, None);
        assert!(
            maintain_cache(&root.0, &[copied_file.clone(), active.clone()], 1, 10, now).is_err()
        );
        assert_eq!(fs::read(copied_file).unwrap(), b"keep");
        assert!(active.exists());
    }

    #[test]
    fn expiry_cleans_abandoned_transfers_and_protects_clipboard_references() {
        let root = TestDirectory::new();
        let now = SystemTime::now();
        let keep = cached_transfer(&root.0, "clipboard", 2, Some(now));
        let expired = cached_transfer(&root.0, "expired", 2, Some(now));
        let abandoned = cached_transfer(&root.0, "abandoned", 2, None);
        let active = cached_transfer(&root.0, "active", 2, None);
        let future = now + CACHE_MAX_AGE + Duration::from_secs(1);
        maintain_cache(&root.0, &[keep.clone(), active.clone()], 0, 10, future).unwrap();
        assert!(keep.exists());
        assert!(active.exists());
        assert!(!expired.exists());
        assert!(!abandoned.exists());
    }

    #[test]
    fn corrupt_size_record_is_not_counted_as_empty() {
        let root = TestDirectory::new();
        let now = SystemTime::now();
        let path = cached_transfer(&root.0, "invalid", 7, Some(now));
        fs::write(path.join(".size"), "invalid").unwrap();
        assert!(maintain_cache(&root.0, std::slice::from_ref(&path), 1, 10, now).is_err());
        maintain_cache(&root.0, &[], 1, 10, now).unwrap();
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn cache_cleanup_does_not_follow_symlinks() {
        use std::os::unix::fs::symlink;

        let root = TestDirectory::new();
        let outside = TestDirectory::new();
        let target = cached_transfer(&outside.0, "outside", 7, Some(SystemTime::now()));
        symlink(&target, root.0.join("link")).unwrap();
        maintain_cache(&root.0, &[], 0, 0, SystemTime::now() + CACHE_MAX_AGE).unwrap();
        assert!(target.join(".size").is_file());
        assert!(
            fs::symlink_metadata(root.0.join("link"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn cache_cleanup_handles_restrictive_directory_permissions() {
        let root = TestDirectory::new();
        let path = cached_transfer(&root.0, "restricted", 7, Some(SystemTime::now()));
        fs::create_dir(path.join("nested")).unwrap();
        fs::write(path.join("nested/file.txt"), b"payload").unwrap();
        fs::set_permissions(path.join("nested"), fs::Permissions::from_mode(0o000)).unwrap();
        maintain_cache(&root.0, &[], 0, 0, SystemTime::now()).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn relative_paths_preserve_mixed_separators_and_portable_names() {
        assert_eq!(
            safe_relative_path(b"folder\\nested/report?.txt").unwrap(),
            PathBuf::from("folder").join("nested").join("report_.txt")
        );
    }

    #[test]
    fn relative_paths_reject_traversal_and_empty_components() {
        for path in [
            b"".as_slice(),
            b"../secret",
            b"folder/../secret",
            b"folder/./item",
            b"/secret",
            b"folder//item",
            b"folder\\\\item",
            b"folder/",
            b"folder\\",
        ] {
            assert!(safe_relative_path(path).is_err(), "accepted {path:?}");
        }
    }

    #[test]
    fn reserved_device_names_are_sanitized_without_changing_similar_names() {
        for (name, expected) in [
            ("com1.txt", "com1_.txt"),
            ("COM9", "COM9_"),
            ("Lpt1.log", "Lpt1_.log"),
            ("LPT9", "LPT9_"),
            ("CON.txt", "CON_.txt"),
            ("COM0", "COM0"),
            ("COM10", "COM10"),
            ("LPT0", "LPT0"),
            ("LPT10", "LPT10"),
        ] {
            assert_eq!(portable_component(name.as_bytes()), expected.as_bytes());
        }
    }
}
