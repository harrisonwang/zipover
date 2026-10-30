#!/usr/bin/env bash
set -euo pipefail

# 与应用保持相同的语言优先级；不支持的系统语言回退到英文。
locale_language() {
  local value=$1
  value=${value#"${value%%[![:space:]]*}"}
  value=${value%"${value##*[![:space:]]}"}
  case "$value" in
    [zZ][hH]|[zZ][hH][-_.@]*) printf 'zh' ;;
    [eE][nN]|[eE][nN][-_.@]*) printf 'en' ;;
    *) return 1 ;;
  esac
}

if ! language=$(locale_language "${ZIPOVER_LANGUAGE:-}"); then
  language=en
  for setting in "${LC_ALL:-}" "${LC_MESSAGES:-}" "${LANG:-}"; do
    setting=${setting#"${setting%%[![:space:]]*}"}
    setting=${setting%"${setting##*[![:space:]]}"}
    [[ -z "$setting" ]] && continue
    language=$(locale_language "$setting") || language=en
    break
  done
fi

message() {
  if [[ "$language" == zh ]]; then
    printf '%s\n' "$2"
  else
    printf '%s\n' "$1"
  fi
}

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  message 'The desktop installer currently supports x86_64 Linux.' '桌面版安装脚本目前支持 x86_64 Linux。' >&2
  exit 1
fi
for command in curl sha256sum; do
  command -v "$command" >/dev/null || { message "Install ${command} first." "请先安装 ${command}。" >&2; exit 1; }
done

releases=https://github.com/harrisonwang/zipover-releases/releases
version=${1:-}
if [[ -z "$version" ]]; then
  version=$(curl -fsSL https://raw.githubusercontent.com/harrisonwang/zipover-releases/main/channels/v1.json \
    | sed -n 's/.*"tag"[[:space:]]*:[[:space:]]*"\(v1\.[0-9]*\.[0-9]*\)".*/\1/p')
fi
if [[ ! "$version" =~ ^v1\.[0-9]+\.[0-9]+(-rc\.[1-9][0-9]*)?$ ]]; then
  message 'Use a version such as v1.0.0 or v1.0.0-rc.1; omit it to install the latest stable 1.x release.' \
    '版本格式应为 v1.0.0 或 v1.0.0-rc.1；不传参数则安装最新 1.x 正式版。' >&2
  exit 1
fi

asset="zipover-gui_${version}_x86_64-unknown-linux-gnu.AppImage"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cd "$stage"
curl -fL --retry 3 "$releases/download/$version/$asset" -o "$asset"
curl -fsSL --retry 3 "$releases/download/$version/SHA256SUMS.txt" -o SHA256SUMS.txt
awk -v name="$asset" '$2 == name { print; found = 1 } END { if (!found) exit 1 }' \
  SHA256SUMS.txt > checksum
sha256sum -c checksum
chmod +x "$asset"
"./$asset" --appimage-extract >/dev/null

data_dir=${XDG_DATA_HOME:-"$HOME/.local/share"}
if [[ "$data_dir" != /* ]]; then
  message 'XDG_DATA_HOME must be an absolute path.' 'XDG_DATA_HOME 必须是绝对路径。' >&2
  exit 1
fi
app_dir="$data_dir/zipover"
mkdir -p "$app_dir" "$data_dir/applications" "$data_dir/icons/hicolor/512x512/apps"
install -m 755 "$asset" "$app_dir/zipover.AppImage.new"
mv -f "$app_dir/zipover.AppImage.new" "$app_dir/zipover.AppImage"
install -m 644 squashfs-root/zipover.png "$data_dir/icons/hicolor/512x512/apps/zipover.png"

# 桌面入口的引号还需转义保留字段代码和 shell 特殊字符。
executable=$(printf '%s' "$app_dir/zipover.AppImage" | sed 's/\\/\\\\\\\\/g; s/"/\\\\"/g; s/`/\\\\`/g; s/\$/\\\\$/g; s/%/%%/g')
cat > "$data_dir/applications/zipover.desktop" <<EOF
[Desktop Entry]
Type=Application
Name=Zipover
Comment=Cross-device clipboard sync
Comment[zh]=跨设备剪贴板同步
Exec=env APPIMAGE_EXTRACT_AND_RUN=1 "$executable"
Icon=zipover
Terminal=false
Categories=Utility;
StartupNotify=false
EOF
if command -v update-desktop-database >/dev/null; then
  update-desktop-database "$data_dir/applications"
fi
message "Installed Zipover ${version}. Open Zipover from the application menu; run this script again to update." \
  "已安装 Zipover ${version}。请从应用菜单打开 Zipover；再次运行此脚本即可更新。"
