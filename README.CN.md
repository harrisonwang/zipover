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

从 [zipover.app/cn/download](https://zipover.app/cn/download/) 下载 **Zipover Desktop 1.1.0**。页面提供 Mac Apple Silicon / Intel、Windows x64、Linux x64 安装包及首次启动、签名限制说明。也可使用以下包管理器命令安装 Desktop；CLI 可按后文说明单独安装。

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

Linux 上的 `zipover copy` 会保持前台运行，让复制的文件可以反复粘贴；复制其他内容后，命令自动退出。按 Ctrl+C 可取消，但可能使刚复制的文件列表失效。

文件缓存为已接收和正在接收的文件数据预留最多 64 GiB 空间。未被引用的传输缓存七天后过期，空间不足时可能提前清理。正在传输或剪贴板仍引用的文件会被保留；可用空间不足时会拒绝新的文件传输。

## 从源码构建

使用最新的 Rust 稳定版。本仓库仅构建 CLI 与 core；Desktop 源码独立维护。

```sh
cargo build --locked --release --workspace
cargo test --locked --workspace --all-targets
```

环境验收测试保持手动执行，不加入 CI。在这台 Mac 与可通过 SSH 访问的 Linux 主机之间验证发现、配对、双向传输和断线重连：

```sh
python3 scripts/verify-lan.py --host my-linux-host
```

两端需安装 Rust 和 rsync，Linux 端还需 `timeout`。测试使用独立设备配置和生成的内容，不改动系统剪贴板；日志和结果保存在 `target/lan-acceptance/`。

组播不可用时，可添加 `--connect <这台Mac的IP>:45919` 验证按 IP 配对与重连；自动发现需不带此参数另行验收。

单独验证局域网接口上的 IPv6 组播发现时，将 `en1` 替换为本机实际接口名：

```sh
ZIPOVER_TEST_IPV6_INTERFACE=en1 cargo test --locked -p zipover-core discovery::tests::ipv6_multicast_discovers_a_connectable_ipv6_service -- --ignored --exact
```

Linux、Windows 的系统剪贴板测试也只手动运行。请使用独立桌面会话；命令和运行条件见 `crates/zipover-core/src/clipboard.rs` 中对应测试的注释。

[MIT 许可证](LICENSE)
