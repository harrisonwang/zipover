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

Download **Zipover Desktop 1.2.0** from [zipover.app/download](https://zipover.app/download/). That page lists Mac Apple Silicon/Intel, Windows x64 and Linux x64 packages, installation instructions and current signing limitations. You can also install Desktop using the package-manager commands below. The CLI is available separately.

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

## Build from source

Use the latest stable Rust toolchain. This repository builds the CLI and core; Desktop source is maintained separately.

```sh
cargo build --locked --release --workspace
cargo test --locked --workspace --all-targets
```

[MIT License](LICENSE)
