mod args;

use anyhow::{Context, Result, bail};
#[cfg(any(target_os = "linux", target_os = "windows"))]
use std::path::PathBuf;
use std::{env, thread, time::Duration};
use zipover_core::{SyncOptions, SyncRuntime};

fn main() {
    if let Err(error) = run() {
        eprintln!("zipover：{error:#}");
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
            println!("已复制 {count} 个路径");
        }
        args::Command::Devices => {
            for peer in zipover_core::paired_devices()? {
                println!("{}  {}", hex::encode(peer.id), peer.name);
            }
        }
        args::Command::Forget(id) => {
            let mut device = [0; 16];
            hex::decode_to_slice(id, &mut device).context("设备 ID 无效")?;
            zipover_core::forget_device(device)?;
            println!("已解除配对");
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
                    .context("无法启动桌面版，请安装 zipover-gui 后重试")?
                    .success()
                {
                    bail!("桌面程序启动失败");
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
    bail!("请先安装 zipover-gui 桌面版")
}

fn sync(options: SyncOptions, code: Option<String>, device: Option<String>) -> Result<()> {
    let runtime = SyncRuntime::start(options)?;
    if let Some(code) = code {
        runtime.join(&code, device)?;
        eprintln!("正在配对…");
        wait_for_pairing(&runtime)?;
    } else if runtime.snapshot().devices.is_empty() {
        runtime.generate_code()?;
        wait_for_pairing(&runtime)?;
    }
    eprintln!("同步已启动。按 Ctrl+C 退出。");
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
                "配对码已更新"
            } else {
                "配对码"
            };
            println!("{heading}：{code}\n在另一台设备运行：zipover --pair {code}");
            if shown_code.is_none() {
                eprintln!("配对码两分钟内有效。配对成功后自动开始同步。");
            }
            shown_code = Some(code.to_owned());
        }
        if let Some(error) = state.error {
            bail!("{error}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}
