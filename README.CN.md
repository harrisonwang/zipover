<p align="center">
  <img src="assets/zipover.png" width="96" alt="Zipover">
</p>

<h1 align="center">Zipover</h1>

<p align="center">
  <strong>这边复制，那边粘贴。</strong><br>
  让你的电脑共用一个剪贴板。
</p>

<p align="center">
  macOS · Windows · Linux<br>
  <a href="https://zipover.app/cn/download/">下载</a> ·
  <a href="README.md">English</a>
</p>

在一台电脑上复制文字、图片、文件或文件夹，就能在另一台电脑上粘贴。Zipover 通过局域网直接连接你的电脑，传输全程加密，剪贴板内容不经过云端。

## 安装

从 [zipover.app/cn/download](https://zipover.app/cn/download/) 下载 **Zipover Desktop 1.0.0**。页面提供 Mac Apple Silicon / Intel、Windows x64、Linux x64 安装包及首次启动、签名限制说明。也可使用以下包管理器命令安装 Desktop；CLI 可按后文说明单独安装。

### macOS

使用 [Homebrew](https://brew.sh)：

```sh
brew install --cask harrisonwang/tap/zipover-gui
```

### Windows

使用 [Scoop](https://scoop.sh)：

```powershell
scoop bucket add harrisonwang https://github.com/harrisonwang/scoop-bucket
scoop install zipover-gui
```

### Linux

```sh
curl -fsSL https://raw.githubusercontent.com/harrisonwang/zipover/main/scripts/install-linux.sh | bash
```

## 命令行

macOS 和 Linux 可通过 Homebrew 安装命令行版：

```sh
brew install harrisonwang/tap/zipover
```

Windows 使用 `scoop install zipover` 单独安装命令行版。

```sh
zipover                    # 开始同步；首次运行会显示配对码
zipover --pair 123456       # 输入另一台电脑上显示的配对码
zipover devices            # 查看已配对的电脑
zipover copy ./file.txt     # 将文件或文件夹放入剪贴板
zipover --help              # 查看完整用法
```

每台电脑只需运行桌面版或命令行版中的一个。Linux 用户请在桌面环境中运行命令行版，以便访问系统剪贴板。

## 从源码构建

使用最新的 Rust 稳定版。本仓库仅构建 CLI 与 core；Desktop 源码独立维护。

```sh
cargo build --locked --release --workspace
cargo test --locked --workspace --all-targets
```

[MIT 许可证](LICENSE)
