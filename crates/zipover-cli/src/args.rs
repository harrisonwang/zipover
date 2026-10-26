use anyhow::{Context, Result, bail};
use std::{ffi::OsString, path::PathBuf};
use zipover_core::SyncOptions;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Help,
    Version,
    Copy(Vec<PathBuf>),
    Run {
        options: SyncOptions,
        tray: bool,
        code: Option<String>,
        device: Option<String>,
    },
    Devices,
    Forget(String),
}

pub(crate) fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Command> {
    let mut args = args.into_iter().peekable();
    match args.peek().and_then(|arg| arg.to_str()) {
        Some("-h" | "--help" | "help") => Ok(Command::Help),
        Some("-V" | "--version") => Ok(Command::Version),
        Some("copy") => {
            args.next();
            let mut paths: Vec<_> = args.collect();
            if paths.len() == 1 && matches!(paths[0].to_str(), Some("-h" | "--help")) {
                return Ok(Command::Help);
            }
            if paths.first().is_some_and(|arg| arg == "--") {
                paths.remove(0);
            }
            if paths.is_empty() {
                bail!("copy 至少需要一个文件或目录路径")
            }
            Ok(Command::Copy(
                paths.into_iter().map(PathBuf::from).collect(),
            ))
        }
        Some("devices") => {
            args.next();
            if args.next().is_some() {
                bail!("devices 不接受参数")
            }
            Ok(Command::Devices)
        }
        Some("forget") => {
            args.next();
            let id = next_text(&mut args, "forget")?;
            if args.next().is_some() {
                bail!("forget 只接受一个设备 ID")
            }
            Ok(Command::Forget(id))
        }
        _ => {
            let mut options = SyncOptions::default();
            let mut tray = false;
            let mut code = None;
            let mut device = None;
            while let Some(arg) = args.next() {
                match arg.to_str() {
                    Some("-h" | "--help") => return Ok(Command::Help),
                    Some("--listen") => {
                        options.listen = next_text(&mut args, "--listen")?
                            .parse()
                            .context("监听地址无效")?
                    }
                    Some("--connect") => options.connect = Some(next_text(&mut args, "--connect")?),
                    Some("--tray") => tray = true,
                    Some("--pair") => code = Some(next_text(&mut args, "--pair")?),
                    Some("--device") => device = Some(next_text(&mut args, "--device")?),
                    _ => bail!("未知参数：{}", arg.to_string_lossy()),
                }
            }
            if device.is_some() && code.is_none() {
                bail!("--device 需要同时提供 --pair 配对码")
            }
            if tray && code.is_some() {
                bail!("--tray 的配对请在桌面面板中完成，不能同时使用 --pair")
            }
            Ok(Command::Run {
                options,
                tray,
                code,
                device,
            })
        }
    }
}

fn next_text(args: &mut impl Iterator<Item = OsString>, option: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("{option} 缺少参数"))?
        .into_string()
        .map_err(|_| anyhow::anyhow!("{option} 必须是有效的 UTF-8"))
}

pub(crate) fn usage() -> &'static str {
    "用法：\n  zipover                      开始同步；首次启动显示配对码\n  zipover --pair <6位数字>      配对并继续同步\n  zipover --tray               启动桌面版\n  zipover devices              查看已配对设备\n  zipover forget <设备ID>       解除配对（先退出同步进程）\n  zipover copy <文件或目录>...\n\n配对成功后，两端自动继续同步，无需重新启动。\n多台设备待配对时，用 --device <设备名或ID> 选择。\n网络参数：--listen <IP:端口> 修改监听，\n--connect <主机:端口> 手动直连（同步时限一个已配对设备）。"
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse_str(args: &[&str]) -> Result<Command> {
        parse(args.iter().map(OsString::from))
    }
    #[test]
    fn no_arguments_start_sync() {
        assert_eq!(
            parse_str(&[]).unwrap(),
            Command::Run {
                options: SyncOptions::default(),
                tray: false,
                code: None,
                device: None,
            }
        );
        assert_eq!(parse_str(&["--help"]).unwrap(), Command::Help);
        assert!(parse_str(&["pair"]).is_err());
        assert!(parse_str(&["sync"]).is_err());
    }
    #[test]
    fn pairing_supports_device_and_network_override() {
        let Command::Run {
            code,
            device,
            options,
            ..
        } = parse_str(&[
            "--pair",
            "001234",
            "--device",
            "hp",
            "--connect",
            "hp.local:45876",
        ])
        .unwrap()
        else {
            panic!()
        };
        assert_eq!(code.as_deref(), Some("001234"));
        assert_eq!(device.as_deref(), Some("hp"));
        assert_eq!(options.connect.as_deref(), Some("hp.local:45876"));
        assert!(parse_str(&["--pair"]).is_err());
        assert!(parse_str(&["--device", "hp"]).is_err());
        assert!(parse_str(&["--tray", "--pair", "001234"]).is_err());
    }
    #[test]
    fn copy_preserves_paths() {
        assert_eq!(
            parse_str(&["copy", "--", "-file", "folder"]).unwrap(),
            Command::Copy(vec!["-file".into(), "folder".into()])
        );
    }
}
