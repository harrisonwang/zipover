use crate::{
    files::{CapturedFiles, capture_files},
    image::{CachedImage, CapturedImage, capture_image},
};
use anyhow::{Context, Result, bail};
use arboard::Clipboard;
#[cfg(target_os = "linux")]
use arboard::SetExtLinux;
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

/// Copies existing files and directories to the system clipboard.
///
/// Paths are resolved to absolute paths before writing. On Linux this blocks
/// until another application replaces the clipboard, keeping its contents
/// available for repeated pastes. Terminating the process can discard them.
pub fn copy_paths(paths: Vec<PathBuf>) -> Result<()> {
    let paths = normalize_copy_paths(paths)?;
    let mut clipboard = Clipboard::new().context(crate::i18n::tr("clipboard.unavailable"))?;
    #[cfg(target_os = "linux")]
    clipboard
        .set()
        .wait()
        .file_list(&paths)
        .context(crate::i18n::tr("clipboard.copy_paths_failed"))?;
    #[cfg(not(target_os = "linux"))]
    set_file_list(&mut clipboard, &paths)
        .context(crate::i18n::tr("clipboard.copy_paths_failed"))?;
    Ok(())
}

fn normalize_copy_paths(paths: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    if paths.is_empty() {
        crate::i18n::localized!(bail, "files.nothing_copied");
    }
    paths
        .into_iter()
        .map(|path| {
            if !path.exists() {
                crate::i18n::localized!(bail, "clipboard.path_missing", path = path.display());
            }
            if !path.is_file() && !path.is_dir() {
                crate::i18n::localized!(bail, "clipboard.unsupported_path", path = path.display());
            }
            path.canonicalize().with_context(|| {
                crate::i18n::localized!(
                    format,
                    "clipboard.resolve_path_failed",
                    path = path.display()
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    use std::fs;

    #[test]
    fn copy_rejects_empty_or_missing_paths_before_accessing_clipboard() {
        assert!(copy_paths(Vec::new()).is_err());
        let missing = env::temp_dir().join(format!(
            "zipover-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        assert!(copy_paths(vec![missing]).is_err());
    }

    #[test]
    fn copy_resolves_relative_paths_before_writing() {
        let relative = PathBuf::from("Cargo.toml");
        assert_eq!(
            normalize_copy_paths(vec![relative.clone()]).unwrap(),
            vec![relative.canonicalize().unwrap()]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "internal fixture for linux_copy_keeps_owner_until_clipboard_is_replaced"]
    fn linux_copy_owner_fixture() {
        if let Some(path) = env::var_os("ZIPOVER_TEST_COPY_PATH") {
            copy_paths(vec![path.into()]).unwrap();
        }
    }

    // Run only on an isolated X server, without a clipboard manager:
    // env -u WAYLAND_DISPLAY ZIPOVER_TEST_CLIPBOARD_ISOLATED=1 xvfb-run -a \
    // cargo test -p zipover-core clipboard::tests::linux_copy_keeps_owner_until_clipboard_is_replaced -- --ignored --exact
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires an isolated X11 clipboard; see the command above this test"]
    fn linux_copy_keeps_owner_until_clipboard_is_replaced() {
        use std::process::{Child, Command, Stdio};

        assert_eq!(
            env::var("ZIPOVER_TEST_CLIPBOARD_ISOLATED").as_deref(),
            Ok("1")
        );
        assert!(env::var_os("DISPLAY").is_some());
        assert!(env::var_os("WAYLAND_DISPLAY").is_none());

        struct Owner(Child);
        impl Drop for Owner {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let path = env::temp_dir().join(format!("zipover-copy-owner-{}", std::process::id()));
        fs::write(&path, b"clipboard owner fixture").unwrap();
        let expected = vec![path.canonicalize().unwrap()];
        let mut owner = Owner(
            Command::new(env::current_exe().unwrap())
                .args([
                    "clipboard::tests::linux_copy_owner_fixture",
                    "--ignored",
                    "--exact",
                ])
                .env("ZIPOVER_TEST_COPY_PATH", &path)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let mut clipboard = Clipboard::new().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while clipboard.get().file_list().ok().as_ref() != Some(&expected) {
            assert!(
                owner.0.try_wait().unwrap().is_none(),
                "copy owner exited before writing"
            );
            assert!(
                Instant::now() < deadline,
                "copy owner never wrote the file list"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(owner.0.try_wait().unwrap().is_none());
        assert_eq!(clipboard.get().file_list().unwrap(), expected);
        assert_eq!(clipboard.get().file_list().unwrap(), expected);

        clipboard.set_text("replacement".to_owned()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = owner.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "copy owner did not exit after replacement"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(clipboard.get_text().unwrap(), "replacement");
        fs::remove_file(path).unwrap();
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "changes the system clipboard; run only in an isolated Windows session"]
    fn copying_files_replaces_stale_text_and_image_formats() {
        use arboard::ImageData;
        use std::borrow::Cow;

        let path = env::temp_dir().join(format!("zipover-copy-formats-{}", std::process::id()));
        fs::write(&path, b"clipboard format fixture").unwrap();
        let mut clipboard = Clipboard::new().unwrap();
        clipboard.set_text("stale text".to_owned()).unwrap();
        copy_paths(vec![path.clone()]).unwrap();
        assert!(clipboard.get_text().is_err());
        assert_eq!(
            normalize_copy_paths(clipboard.get().file_list().unwrap()).unwrap(),
            vec![path.canonicalize().unwrap()]
        );

        clipboard
            .set_image(ImageData {
                width: 1,
                height: 1,
                bytes: Cow::Owned(vec![255, 0, 0, 255]),
            })
            .unwrap();
        copy_paths(vec![path.clone()]).unwrap();
        assert!(clipboard.get_image().is_err());
        assert_eq!(
            normalize_copy_paths(clipboard.get().file_list().unwrap()).unwrap(),
            vec![path.canonicalize().unwrap()]
        );
        clipboard.clear().unwrap();
        fs::remove_file(path).unwrap();
    }
}
