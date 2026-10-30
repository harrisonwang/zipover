use crate::tr;
use anyhow::{Context, Result, anyhow, bail};
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
                localized!(bail, "args.copy-path-required")
            }
            Ok(Command::Copy(
                paths.into_iter().map(PathBuf::from).collect(),
            ))
        }
        Some("devices") => {
            args.next();
            if args.next().is_some() {
                localized!(bail, "args.devices-no-arguments")
            }
            Ok(Command::Devices)
        }
        Some("forget") => {
            args.next();
            let id = next_text(&mut args, "forget")?;
            if args.next().is_some() {
                localized!(bail, "args.forget-single-id")
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
                            .context(tr("args.invalid-listen"))?
                    }
                    Some("--connect") => options.connect = Some(next_text(&mut args, "--connect")?),
                    Some("--tray") => tray = true,
                    Some("--pair") => code = Some(next_text(&mut args, "--pair")?),
                    Some("--device") => device = Some(next_text(&mut args, "--device")?),
                    _ => localized!(bail, "args.unknown", argument = arg.to_string_lossy()),
                }
            }
            if device.is_some() && code.is_none() {
                localized!(bail, "args.device-requires-code")
            }
            if tray && code.is_some() {
                localized!(bail, "args.tray-pair-conflict")
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
        .with_context(|| localized!(format, "args.missing-value", option = option))?
        .into_string()
        .map_err(|_| localized!(anyhow, "args.invalid-utf8", option = option))
}

pub(crate) fn usage() -> std::borrow::Cow<'static, str> {
    tr("args.usage")
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
            "hp.local:57321",
        ])
        .unwrap()
        else {
            panic!()
        };
        assert_eq!(code.as_deref(), Some("001234"));
        assert_eq!(device.as_deref(), Some("hp"));
        assert_eq!(options.connect.as_deref(), Some("hp.local:57321"));
        assert!(parse_str(&["--pair"]).is_err());
        assert!(parse_str(&["--device", "hp"]).is_err());
        assert!(parse_str(&["--tray", "--pair", "001234"]).is_err());
    }

    #[test]
    fn listen_defaults_and_explicit_overrides_are_preserved() {
        for tray in [false, true] {
            let mut args = Vec::new();
            if tray {
                args.push("--tray");
            }
            let Command::Run { options, .. } = parse_str(&args).unwrap() else {
                panic!()
            };
            assert_eq!(options.listen, "[::]:57321".parse().unwrap());
            args.extend(["--listen", "127.0.0.1:0"]);
            let Command::Run { options, .. } = parse_str(&args).unwrap() else {
                panic!()
            };
            assert_eq!(options.listen, "127.0.0.1:0".parse().unwrap());
        }
    }
    #[test]
    fn copy_preserves_paths() {
        assert_eq!(
            parse_str(&["copy", "--", "-file", "folder"]).unwrap(),
            Command::Copy(vec!["-file".into(), "folder".into()])
        );
    }
}
