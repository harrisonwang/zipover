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
use std::os::unix::fs::PermissionsExt;

use super::transport::PeerHub;
use crate::{
    protocol::{CHUNK_SIZE, MAX_MANIFEST_ENTRIES, MAX_TRANSFER_BYTES, WireMessage},
    sync::format_bytes,
};
const CACHE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const CACHE_DIRECTORY_NAME: &str = "zipover-sync";

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
        bail!("复制项目为空")
    }

    let internal = paths.iter().any(|path| is_internal_cache_path(path));
    let mut entries = Vec::new();
    let mut names = HashSet::new();

    for path in paths {
        let root_name = path
            .file_name()
            .context("无法读取复制项目的文件名")?
            .to_owned();
        capture_entry(path, PathBuf::from(root_name), &mut entries, &mut names)?;
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
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
        bail!("目录项目数量超过限制（最多 {MAX_MANIFEST_ENTRIES} 项）")
    }

    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("读取复制项目失败：{}", source.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("暂不支持符号链接：{}", source.display())
    }

    let wire_path = portable_relative_path(&relative)?;
    if !names.insert(path_collision_key(&wire_path)) {
        bail!("复制项目存在重名路径：{}", path_display(&wire_path))
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
        bail!("暂不支持特殊文件：{}", source.display())
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
            .with_context(|| format!("读取目录失败：{}", source.display()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("读取目录内容失败：{}", source.display()))?;
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
    let cache_root = env::temp_dir().join(CACHE_DIRECTORY_NAME);
    if path.starts_with(&cache_root) {
        return true;
    }

    // macOS 可能把 /var 规范化为 /private/var，比较规范化后的路径才能识别缓存。
    let Some(canonical_path) = path.canonicalize().ok() else {
        return false;
    };
    let Some(canonical_root) = cache_root.canonicalize().ok() else {
        return false;
    };
    canonical_path.starts_with(canonical_root)
}

fn manifest_identity(directory: &Path, entries: &[TransferEntry]) -> Result<u64> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for entry in entries {
        entry.hash(&mut hasher);
        let path = directory.join(safe_relative_path(&entry.path)?);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("读取接收文件元数据失败：{}", path.display()))?;
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
        total.checked_add(entry.size).context("文件总大小溢出")
    })?;
    if total_bytes > MAX_TRANSFER_BYTES {
        bail!(
            "文件总大小超过限制（最多 {} GiB）",
            MAX_TRANSFER_BYTES / (1024 * 1024 * 1024)
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

        let source = local.source.as_ref().context("文件来源路径丢失")?;
        eprintln!(
            "剪贴板同步：正在读取文件 {}",
            path_display(&local.entry.path)
        );
        let before = fs::symlink_metadata(source)
            .with_context(|| format!("读取文件元数据失败：{}", source.display()))?;
        if !before.is_file()
            || before.len() != local.entry.size
            || file_mode(&before) != local.entry.mode
            || modified_nanos(&before) != local.modified_nanos
        {
            bail!("文件在传输前发生变化：{}", source.display())
        }
        let mut file =
            File::open(source).with_context(|| format!("打开文件失败：{}", source.display()))?;
        let mut hasher = Sha256::new();
        let mut offset = 0u64;

        loop {
            let length = file
                .read(&mut buffer)
                .with_context(|| format!("读取文件失败：{}", source.display()))?;
            if length == 0 {
                break;
            }

            let length_u64 = u64::try_from(length).context("文件分块大小溢出")?;
            let next_offset = offset.checked_add(length_u64).context("文件大小溢出")?;
            if next_offset > local.entry.size {
                bail!("文件在传输过程中变大：{}", source.display())
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
            bail!("文件在传输过程中发生变化：{}", source.display())
        }
        let after = fs::symlink_metadata(source)
            .with_context(|| format!("读取文件元数据失败：{}", source.display()))?;
        if !after.is_file()
            || after.len() != local.entry.size
            || file_mode(&after) != local.entry.mode
            || modified_nanos(&after) != local.modified_nanos
        {
            bail!("文件在传输过程中发生变化：{}", source.display())
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
        file_index = file_index.checked_add(1).context("文件数量溢出")?;
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
            bail!("传输清单为空")
        }
        if entries.len() > MAX_MANIFEST_ENTRIES {
            bail!("传输清单项目数量超过限制")
        }
        let total_bytes = entries.iter().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.size)
                .context("传输清单文件大小溢出")
        })?;
        if total_bytes > MAX_TRANSFER_BYTES {
            bail!("传输文件总大小超过限制")
        }

        let directory = env::temp_dir()
            .join(CACHE_DIRECTORY_NAME)
            .join(transfer_component(&format!("{cache_scope}-{id}")));
        let parts_directory = directory.join(".parts");
        fs::create_dir_all(&parts_directory)
            .with_context(|| format!("创建临时传输目录失败：{}", directory.display()))?;

        let result = Self::start_inner(directory.clone(), parts_directory, entries);
        if result.is_err() {
            let _ = fs::remove_dir_all(&directory);
        }
        result
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
                bail!("传输清单存在重复路径：{}", path_display(&entry.path))
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
                    bail!(
                        "传输清单中，文件不能作为目录父级：{}",
                        path_display(&entry.path)
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
                        bail!("目录大小必须为 0：{}", path_display(&entry.path))
                    }
                    fs::create_dir_all(&final_path)
                        .with_context(|| format!("创建目录失败：{}", final_path.display()))?;
                }
                TransferEntryKind::File => {
                    let parent = final_path.parent().context("接收文件缺少父目录")?;
                    fs::create_dir_all(parent)
                        .with_context(|| format!("创建文件父目录失败：{}", parent.display()))?;
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
            bail!("传输清单没有顶层路径")
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
            bail!("收到的文件分块大小无效")
        }
        let file = self
            .files
            .get_mut(file_index as usize)
            .context("文件分块索引无效")?;
        if file.finished {
            bail!("文件已经结束，不能继续写入")
        }
        if offset != file.received_size {
            bail!(
                "文件分块偏移不连续：期望 {}，实际 {}",
                file.received_size,
                offset
            )
        }
        let length = u64::try_from(bytes.len()).context("文件分块大小溢出")?;
        let next_size = file
            .received_size
            .checked_add(length)
            .context("接收文件大小溢出")?;
        if next_size > file.expected_size {
            bail!("接收文件大小超过清单声明")
        }

        if file.file.is_none() {
            file.file = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&file.temporary_path)
                    .with_context(|| {
                        format!("创建临时文件失败：{}", file.temporary_path.display())
                    })?,
            );
        }
        let handle = file.file.as_mut().context("接收文件句柄已关闭")?;
        handle.write_all(bytes).context("写入文件分块失败")?;
        file.hasher.update(bytes);
        file.received_size = next_size;
        Ok(())
    }

    pub(crate) fn finish_file(&mut self, file_index: u32, expected_sha256: [u8; 32]) -> Result<()> {
        let file = self
            .files
            .get_mut(file_index as usize)
            .context("文件结束索引无效")?;
        if file.finished {
            bail!("文件已经结束")
        }
        if file.received_size != file.expected_size {
            bail!(
                "文件大小不匹配：期望 {}，实际 {}",
                file.expected_size,
                file.received_size
            )
        }

        let actual_sha256: [u8; 32] = file.hasher.clone().finalize().into();
        if actual_sha256 != expected_sha256 {
            bail!("SHA-256 校验不匹配")
        }

        let mut handle = match file.file.take() {
            Some(handle) => handle,
            None => OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file.temporary_path)
                .with_context(|| format!("创建空文件失败：{}", file.temporary_path.display()))?,
        };
        handle.flush().context("刷新临时文件失败")?;
        drop(handle);
        fs::hard_link(&file.temporary_path, &file.final_path).with_context(|| {
            format!(
                "将临时文件提交为最终文件失败：{}",
                file.final_path.display()
            )
        })?;
        fs::remove_file(&file.temporary_path)
            .with_context(|| format!("清理临时文件失败：{}", file.temporary_path.display()))?;
        file.finished = true;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<FinishedTransfer> {
        if self.files.iter().any(|file| !file.finished) {
            bail!("仍有文件没有完成校验")
        }
        // 先读取元数据计算指纹，再恢复目录权限，避免无权进入目录时无法完成收尾。
        let contents_directory = self.directory.join("contents");
        let local_fingerprint = manifest_identity(&contents_directory, &self.entries)?;
        for entry in self.entries.iter().rev() {
            let path = contents_directory.join(safe_relative_path(&entry.path)?);
            apply_mode(&path, entry.mode)?;
        }
        if self.parts_directory.exists() {
            fs::remove_dir_all(&self.parts_directory).with_context(|| {
                format!("清理临时分块目录失败：{}", self.parts_directory.display())
            })?;
        }
        self.committed = true;
        Ok(FinishedTransfer {
            fingerprint: local_fingerprint,
            roots: self.roots.clone(),
            directory: self.directory.clone(),
        })
    }

    pub(crate) fn summary(&self) -> String {
        let file_count = self.files.len();
        let directory_count = self
            .entries
            .iter()
            .filter(|entry| matches!(entry.kind, TransferEntryKind::Directory))
            .count();
        let total_bytes = self.entries.iter().map(|entry| entry.size).sum::<u64>();
        format!(
            "{} 个文件、{} 个目录（{}）",
            file_count,
            directory_count,
            format_bytes(total_bytes)
        )
    }
}

impl Drop for IncomingTransfer {
    fn drop(&mut self) {
        if !self.committed {
            self.files.clear();
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

impl FinishedTransfer {
    pub(crate) fn cleanup(self) {
        let _ = fs::remove_dir_all(self.directory);
    }
}
pub(crate) fn files_summary(files: &CapturedFiles) -> String {
    let file_count = files
        .entries
        .iter()
        .filter(|entry| matches!(entry.entry.kind, TransferEntryKind::File))
        .count();
    let directory_count = files
        .entries
        .iter()
        .filter(|entry| matches!(entry.entry.kind, TransferEntryKind::Directory))
        .count();
    let total_bytes = files
        .entries
        .iter()
        .map(|entry| entry.entry.size)
        .sum::<u64>();
    format!(
        "{} 个文件、{} 个目录（{}）",
        file_count,
        directory_count,
        format_bytes(total_bytes)
    )
}

pub(crate) fn portable_relative_path(path: &Path) -> Result<Vec<u8>> {
    let mut components = Vec::new();
    for component in path.components() {
        let component = os_str_bytes(component.as_os_str());
        if component.is_empty() || component == b"." || component == b".." {
            bail!("复制路径包含无效组件")
        }
        components.push(portable_component(&component));
    }
    if components.is_empty() {
        bail!("复制路径为空")
    }
    Ok(components.join(&b'/'))
}

pub(crate) fn safe_relative_path(path: &[u8]) -> Result<PathBuf> {
    let mut result = PathBuf::new();
    let mut component = Vec::new();
    for byte in path {
        if *byte == b'/' || *byte == b'\\' {
            push_safe_component(&mut component, &mut result, path)?;
        } else {
            component.push(*byte);
        }
    }
    push_safe_component(&mut component, &mut result, path)?;
    if result.as_os_str().is_empty() {
        bail!("传输路径为空")
    }
    Ok(result)
}

fn push_safe_component(
    component: &mut Vec<u8>,
    result: &mut PathBuf,
    full_path: &[u8],
) -> Result<()> {
    if component.is_empty() || component == b"." || component == b".." {
        bail!("传输路径包含无效组件：{}", path_display(full_path))
    }
    result.push(os_string_from_bytes(&portable_component(component))?);
    component.clear();
    Ok(())
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
            | b"COM1"
            | b"COM2"
            | b"COM3"
            | b"COM4"
            | b"COM5"
            | b"COM6"
            | b"COM7"
            | b"COM8"
            | b"COM9"
            | b"LPT1"
            | b"LPT2"
            | b"LPT3"
            | b"LPT4"
            | b"LPT5"
            | b"LPT6"
            | b"LPT7"
            | b"LPT8"
            | b"LPT9"
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
            .context("Windows 无法表示非 UTF-8 文件名")
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

pub(crate) fn cleanup_stale_cache() {
    let directory = env::temp_dir().join(CACHE_DIRECTORY_NAME);
    let Ok(entries) = fs::read_dir(&directory) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        let is_old = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .and_then(|modified| now.duration_since(modified).map_err(std::io::Error::other))
            .map(|age| age >= CACHE_MAX_AGE)
            .unwrap_or(false);
        if is_old && fs::remove_dir_all(&path).is_ok() {
            eprintln!("剪贴板同步：已清理过期临时目录 {}", path.display());
        }
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
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("恢复文件权限失败：{}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn apply_mode(_path: &Path, _mode: Option<u32>) -> Result<()> {
    Ok(())
}
