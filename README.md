<p align="center">
  <img src="assets/zipover.png" width="96" alt="Zipover">
</p>

<h1 align="center">Zipover</h1>

<p align="center">
  <strong>Copy here. Paste there.</strong><br>
  A private clipboard for your computers.
</p>

<p align="center">
  macOS · Windows · Linux<br>
  <a href="https://zipover.app/download/">Download</a> ·
  <a href="README.CN.md">简体中文</a>
</p>

Copy text, images, files, or folders on one computer and paste them on another. Zipover connects your computers directly over your local network, with encrypted transfers. Clipboard contents never pass through a cloud server.

## Install

Download **Zipover Desktop 1.0.0** from [zipover.app/download](https://zipover.app/download/). That page lists Mac Apple Silicon/Intel, Windows x64 and Linux x64 packages, installation instructions and current signing limitations. You can also install Desktop using the package-manager commands below. The CLI is available separately.

### macOS

Using [Homebrew](https://brew.sh):

```sh
brew install --cask harrisonwang/tap/zipover-gui
```

### Windows

Using [Scoop](https://scoop.sh):

```powershell
scoop bucket add harrisonwang https://github.com/harrisonwang/scoop-bucket
scoop install zipover-gui
```

### Linux

```sh
curl -fsSL https://raw.githubusercontent.com/harrisonwang/zipover/main/scripts/install-linux.sh | bash
```

## Command line

On macOS and Linux, install the CLI with Homebrew:

```sh
brew install harrisonwang/tap/zipover
```

On Windows, install the CLI with `scoop install zipover`.

```sh
zipover                    # Start syncing; first run shows a pairing code
zipover --pair 123456       # Enter the code shown on another computer
zipover devices            # List paired computers
zipover copy ./file.txt     # Copy a file or folder to the clipboard
zipover --help              # Show all options
```

Run one instance per computer, using either the desktop app or the CLI. On Linux, the CLI needs an active desktop session to access the clipboard.

On Linux, `zipover copy` stays in the foreground so the copied files remain available for repeated pastes. Copying other content ends the command. Pressing Ctrl+C cancels it and may discard the copied file list.

The file cache reserves up to 64 GiB for received and incoming file data. Unreferenced transfers expire after seven days and may be removed sooner when space is needed. Active transfers and files still on the clipboard are kept; new file transfers are refused if there is insufficient space.

## Build from source

Use the latest stable Rust toolchain. This repository builds the CLI and core; Desktop source is maintained separately.

```sh
cargo build --locked --release --workspace
cargo test --locked --workspace --all-targets
```

Environment acceptance tests run manually and stay out of CI. To test discovery, pairing, bidirectional transfers and reconnection between this Mac and an SSH-accessible Linux host, run:

```sh
python3 scripts/verify-lan.py --host my-linux-host
```

Both hosts need Rust and rsync; the Linux host also needs `timeout`. The test uses isolated device settings and generated content, leaving the system clipboard untouched. Logs and results are written under `target/lan-acceptance/`.

When multicast is unavailable, add `--connect <this-Mac-IP>:45919` to verify pairing and reconnection by IP. Automatic discovery requires a separate run without that option.

To verify IPv6 multicast discovery on a LAN interface, replace `en1` with the interface name on your computer:

```sh
ZIPOVER_TEST_IPV6_INTERFACE=en1 cargo test --locked -p zipover-core discovery::tests::ipv6_multicast_discovers_a_connectable_ipv6_service -- --ignored --exact
```

The Linux and Windows system clipboard tests are also manual. Run them only in an isolated desktop session; commands and prerequisites are documented next to the tests in `crates/zipover-core/src/clipboard.rs`.

[MIT License](LICENSE)
