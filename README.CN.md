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

CLI 与 core 免费开源。Desktop 为独立商业软件：一次购买覆盖一个大版本、最多 5 台设备，仅主动激活或解绑时需要联网，激活后可长期离线使用，无需定期验证。

Desktop 提供 **30 天离线试用**，从点击「开始 30 天免费试用」起算，无需账号或付款。到期暂停同步，保留配对信息，不会自动扣费。购买暂未开放。

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

安装脚本目前支持 x86_64 Linux。使用 GNOME 桌面时，需要启用 AppIndicator 扩展才能显示托盘图标。

## 配对后，照常复制粘贴

1. 在两台电脑上打开 Zipover，分别开始免费试用，并确保它们连接同一局域网。
2. 点击菜单栏或托盘中的 Zipover 图标，选择「添加设备」。在一台电脑上查看「本机配对码」，再到另一台电脑的「添加设备」窗口输入这六位数字。
3. 配对成功后，让两台电脑上的 Zipover 保持运行。照常复制，再到另一台电脑粘贴即可。

只需配对一次，下次打开无需重新输入配对码。使用三台及以上电脑时，需要互传内容的每两台电脑都应直接配对。例如，A、B、C 三台电脑需要完成 A↔B、A↔C、B↔C 三组配对；文件和文件夹不会经中间电脑转发。

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
