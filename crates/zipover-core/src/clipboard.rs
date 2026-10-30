use crate::{
    files::{CapturedFiles, capture_files},
    image::{CachedImage, CapturedImage, apply_image, capture_image},
    protocol::ClipboardPayload,
};
use anyhow::{Context, Result, bail};
use arboard::Clipboard;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
const FILE_CACHE_VALIDATION_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) enum CapturedPayload {
    Text(String),
    Image(CapturedImage),
    Files(Arc<CapturedFiles>),
}

#[derive(Default)]
pub(crate) struct CaptureCache {
    files: Option<(Vec<PathBuf>, Arc<CapturedFiles>)>,
    next_file_validation: Option<Instant>,
    image: Option<CachedImage>,
    last_file_error: Option<String>,
}

pub(crate) fn capture_payload(
    clipboard: &mut Clipboard,
    cache: &mut CaptureCache,
) -> Result<CapturedPayload> {
    if let Ok(paths) = clipboard.get().file_list()
        && !paths.is_empty()
    {
        let cached = cache
            .files
            .as_ref()
            .filter(|(cached_paths, _)| *cached_paths == paths)
            .map(|(_, files)| files.clone());
        if let Some(files) = cached
            && cache
                .next_file_validation
                .is_some_and(|deadline| deadline > Instant::now())
        {
            return Ok(CapturedPayload::Files(files));
        }

        match capture_files(&paths) {
            Ok(files) => {
                let files = Arc::new(files);
                cache.files = Some((paths, files.clone()));
                cache.next_file_validation = Some(Instant::now() + FILE_CACHE_VALIDATION_INTERVAL);
                cache.last_file_error = None;
                return Ok(CapturedPayload::Files(files));
            }
            Err(error) => {
                let message = format!("{error:#}");
                if cache.last_file_error.as_deref() != Some(&message) {
                    crate::i18n::localized!(
                        eprintln,
                        "clipboard.invalid_file_list",
                        message = message
                    );
                    cache.last_file_error = Some(message);
                }
            }
        }
    }
    cache.files = None;
    cache.next_file_validation = None;

    if let Some(image) = capture_image(clipboard, &mut cache.image)? {
        return Ok(CapturedPayload::Image(image));
    }
    if let Ok(text) = clipboard.get_text()
        && !text.is_empty()
    {
        cache.last_file_error = None;
        return Ok(CapturedPayload::Text(text));
    }
    crate::i18n::localized!(bail, "clipboard.unsupported_content")
}

pub(crate) fn apply_payload(clipboard: &mut Clipboard, payload: &ClipboardPayload) -> Result<()> {
    match payload {
        ClipboardPayload::Text(text) => clipboard
            .set_text(text)
            .context(crate::i18n::tr("clipboard.write_text_failed"))?,
        ClipboardPayload::Image {
            width,
            height,
            bytes,
        } => apply_image(clipboard, *width, *height, bytes)?,
    }
    Ok(())
}

pub(crate) fn set_file_list(clipboard: &mut Clipboard, paths: &[PathBuf]) -> Result<()> {
    #[cfg(windows)]
    clipboard
        .clear()
        .context(crate::i18n::tr("clipboard.clear_failed"))?;
    clipboard
        .set()
        .file_list(paths)
        .context(crate::i18n::tr("clipboard.write_files_failed"))?;
    Ok(())
}

pub fn copy_paths(paths: Vec<PathBuf>) -> Result<()> {
    for path in &paths {
        if !path.exists() {
            crate::i18n::localized!(bail, "clipboard.path_missing", path = path.display());
        }
        if !path.is_file() && !path.is_dir() {
            crate::i18n::localized!(bail, "clipboard.unsupported_path", path = path.display());
        }
    }

    let mut clipboard = Clipboard::new().context(crate::i18n::tr("clipboard.unavailable"))?;
    clipboard
        .set()
        .file_list(&paths)
        .context(crate::i18n::tr("clipboard.copy_paths_failed"))?;

    Ok(())
}
