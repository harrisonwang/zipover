rust_i18n::i18n!("locales", fallback = "en");

macro_rules! localized {
    ($operation:ident, $key:literal $(, $($arguments:tt)*)?) => {
        $operation!("{}", rust_i18n::t!($key $(, $($arguments)*)?))
    };
}

mod args;

use anyhow::{Context, Result, bail};
#[cfg(any(target_os = "linux", target_os = "windows"))]
use std::path::PathBuf;
use std::{env, thread, time::Duration};
use zipover_core::{SyncOptions, SyncRuntime};

fn tr(key: &'static str) -> std::borrow::Cow<'static, str> {
    rust_i18n::t!(key)
}

fn main() {
    zipover_core::i18n::set_locale(&zipover_core::i18n::locale_from_env(
        &rust_i18n::available_locales!(),
    ));
    if let Err(error) = run() {
        localized!(eprintln, "startup.failed", error = error : {:#});
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match args::parse(env::args_os().skip(1))? {
        args::Command::Help => println!("{}", args::usage()),
        args::Command::Version => println!("zipover {}", env!("CARGO_PKG_VERSION")),
        args::Command::Copy(paths) => {
            let count = paths.len();
            zipover_core::copy_paths(paths)?;
            localized!(println, "copy.completed", count = count);
        }
        args::Command::Devices => {
            for peer in zipover_core::paired_devices()? {
                println!("{}  {}", hex::encode(peer.id), peer.name);
            }
        }
        args::Command::Forget(id) => {
            let mut device = [0; 16];
            hex::decode_to_slice(id, &mut device).context(tr("device.invalid-id"))?;
            zipover_core::forget_device(device)?;
            localized!(println, "device.unpaired");
        }
        args::Command::Run {
            code,
            device,
            options,
            tray,
        } => {
            if tray {
                let mut command = desktop_command()?;
                command.arg("--listen").arg(options.listen.to_string());
                if let Some(address) = options.connect {
                    command.arg("--connect").arg(address);
                }
                if !command
                    .status()
                    .context(tr("desktop.start-failed"))?
                    .success()
                {
                    localized!(bail, "desktop.exit-failed");
                }
            } else {
                sync(options, code, device)?;
            }
        }
    }
    Ok(())
}

fn desktop_command() -> Result<std::process::Command> {
    let cli = env::current_exe()?;
    let name = if cfg!(windows) {
        "zipover-gui.exe"
    } else {
        "zipover-gui"
    };
    let mut candidates = vec![cli.with_file_name(name)];
    if let Some(path) = env::var_os("PATH") {
        candidates.extend(env::split_paths(&path).map(|directory| directory.join(name)));
    }
    #[cfg(target_os = "macos")]
    candidates.push(
        cli.with_file_name("Zipover.app")
            .join("Contents/MacOS/zipover-gui"),
    );
    #[cfg(target_os = "windows")]
    if let Some(scoop) = env::var_os("SCOOP")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(|path| PathBuf::from(path).join("scoop")))
    {
        candidates.push(scoop.join("apps/zipover-gui/current/zipover-gui.exe"));
    }
    #[cfg(target_os = "linux")]
    if let Some(data) = env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|path| PathBuf::from(path).join(".local/share")))
    {
        candidates.push(data.join("zipover/zipover.AppImage"));
    }
    if let Some(executable) = candidates.into_iter().find(|path| path.is_file()) {
        let mut command = std::process::Command::new(executable);
        if cfg!(target_os = "linux") {
            command.env("APPIMAGE_EXTRACT_AND_RUN", "1");
        }
        return Ok(command);
    }
    #[cfg(target_os = "macos")]
    {
        let mut command = std::process::Command::new("open");
        command.args(["-W", "-b", "com.harrisonwang.zipover", "--args"]);
        Ok(command)
    }
    #[cfg(not(target_os = "macos"))]
    localized!(bail, "desktop.install-required")
}

fn sync(options: SyncOptions, code: Option<String>, device: Option<String>) -> Result<()> {
    let runtime = SyncRuntime::start(options)?;
    localized!(eprintln, "sync.listening", address = runtime.local_addr());
    if let Some(code) = code {
        runtime.join(&code, device)?;
        localized!(eprintln, "pairing.started");
        wait_for_pairing(&runtime)?;
    } else if runtime.snapshot().devices.is_empty() {
        runtime.generate_code()?;
        wait_for_pairing(&runtime)?;
    }
    localized!(eprintln, "sync.started");
    runtime.wait()
}

fn wait_for_pairing(runtime: &SyncRuntime) -> Result<()> {
    let mut shown_code = None;
    loop {
        let state = runtime.snapshot();
        if state.paired {
            runtime.cancel_pairing();
            return Ok(());
        }
        if let Some(code) = state.code.as_deref()
            && shown_code.as_deref() != Some(code)
        {
            let heading = if shown_code.is_some() {
                tr("pairing.code-refreshed")
            } else {
                tr("pairing.code")
            };
            localized!(
                println,
                "pairing.instructions",
                heading = heading,
                code = code
            );
            for address in &state.pairing_addresses {
                localized!(
                    println,
                    "pairing.manual-instructions",
                    code = code,
                    address = address
                );
            }
            if shown_code.is_none() {
                localized!(eprintln, "pairing.validity");
            }
            shown_code = Some(code.to_owned());
        }
        if let Some(error) = state.error {
            bail!("{error}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}
