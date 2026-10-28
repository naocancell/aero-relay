#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
output_dir="$repo_root/target/linux-package/media"
cache_dir="$repo_root/target/linux-media-cache"

while (( $# > 0 )); do
  case "$1" in
    --output-dir)
      [[ $# -ge 2 ]] || { echo "--output-dir requires a path" >&2; exit 2; }
      output_dir="$2"
      shift 2
      ;;
    --cache-dir)
      [[ $# -ge 2 ]] || { echo "--cache-dir requires a path" >&2; exit 2; }
      cache_dir="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'USAGE'
Usage: stage-media-tools.sh [--output-dir PATH] [--cache-dir PATH]

Stage prebuilt Linux FFmpeg and MediaMTX executables without source compilation.
USAGE
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

case "$output_dir" in
  /*) ;;
  *) output_dir="$PWD/$output_dir" ;;
esac
case "$cache_dir" in
  /*) ;;
  *) cache_dir="$PWD/$cache_dir" ;;
esac

# shellcheck disable=SC1091
source "$repo_root/deploy/linux/media-versions.env"

# Detect target architecture
target_arch_detected="${TARGET_ARCH:-$(dpkg --print-architecture 2>/dev/null || uname -m)}"
case "$target_arch_detected" in
  arm64|aarch64)
    arch_key="linux-arm64"
    ;;
  amd64|x86_64)
    arch_key="linux-amd64"
    ;;
  *)
    echo "[-] 不支持的架构: $target_arch_detected" >&2
    exit 1
    ;;
esac

prebuilt_dir="$repo_root/deploy/media-bin/$arch_key"
mkdir -p "$output_dir/ffmpeg" "$output_dir/mediamtx" "$cache_dir" "$prebuilt_dir"

ffmpeg_bin="$prebuilt_dir/ffmpeg"
mediamtx_bin="$prebuilt_dir/mediamtx"

# 1. 检查预编译二进制是否存在
if [[ -f "$ffmpeg_bin" && -f "$mediamtx_bin" ]]; then
  printf 'Using prebuilt media tools from %s\n' "$prebuilt_dir"
  install -m 0755 "$ffmpeg_bin" "$output_dir/ffmpeg/ffmpeg"
  install -m 0755 "$mediamtx_bin" "$output_dir/mediamtx/mediamtx"
else
  # 如果预编译目录缺少文件，尝试从缓存或网络获取预编译包
  printf 'Prebuilt media tools incomplete in %s, checking cache or downloading...\n' "$prebuilt_dir"

  # MediaMTX
  if [[ ! -f "$mediamtx_bin" ]]; then
    mediamtx_archive="$cache_dir/$MEDIAMTX_ARCHIVE_NAME"
    if [[ ! -f "$mediamtx_archive" ]]; then
      printf 'Downloading MediaMTX: %s\n' "$MEDIAMTX_ARCHIVE_URL"
      curl --fail --location --retry 2 --connect-timeout 15 -o "$mediamtx_archive" "$MEDIAMTX_ARCHIVE_URL" || {
        printf 'Retrying with mirror...\n'
        curl --fail --location --retry 2 --connect-timeout 15 -o "$mediamtx_archive" "https://ghproxy.net/$MEDIAMTX_ARCHIVE_URL"
      }
    fi
    tar -xzf "$mediamtx_archive" -C "$prebuilt_dir" mediamtx
    chmod +x "$mediamtx_bin"
  fi

  # FFmpeg (使用静态单文件发布包，免编译)
  if [[ ! -f "$ffmpeg_bin" ]]; then
    case "$arch_key" in
      linux-amd64)
        ffmpeg_url="https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
        ffmpeg_archive="$cache_dir/ffmpeg-release-amd64-static.tar.xz"
        ;;
      linux-arm64)
        ffmpeg_url="https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz"
        ffmpeg_archive="$cache_dir/ffmpeg-release-arm64-static.tar.xz"
        ;;
    esac

    if [[ ! -f "$ffmpeg_archive" ]]; then
      printf 'Downloading prebuilt static FFmpeg: %s\n' "$ffmpeg_url"
      curl --fail --location --retry 2 --connect-timeout 15 -o "$ffmpeg_archive" "$ffmpeg_url"
    fi

    tmp_extract="$(mktemp -d "/tmp/ffmpeg-extract.XXXXXX")"
    tar -xf "$ffmpeg_archive" -C "$tmp_extract"
    find "$tmp_extract" -type f -name ffmpeg -exec cp -f {} "$ffmpeg_bin" \;
    rm -rf "$tmp_extract"
    chmod +x "$ffmpeg_bin"
  fi

  install -m 0755 "$ffmpeg_bin" "$output_dir/ffmpeg/ffmpeg"
  install -m 0755 "$mediamtx_bin" "$output_dir/mediamtx/mediamtx"
fi

# 验证可执行性及版本输出
if "$output_dir/ffmpeg/ffmpeg" -version >/dev/null 2>&1; then
  ffmpeg_version_line="$("$output_dir/ffmpeg/ffmpeg" -version 2>&1 | sed -n '1p')"
  printf 'Staged FFmpeg: %s\n' "$ffmpeg_version_line"
else
  printf 'Staged FFmpeg: %s (non-native or static execution check skipped)\n' "$output_dir/ffmpeg/ffmpeg"
fi

if "$output_dir/mediamtx/mediamtx" --version >/dev/null 2>&1; then
  mediamtx_version_line="$("$output_dir/mediamtx/mediamtx" --version 2>&1 | sed -n '1p')"
  printf 'Staged MediaMTX: %s\n' "$mediamtx_version_line"
else
  printf 'Staged MediaMTX: %s (non-native or static execution check skipped)\n' "$output_dir/mediamtx/mediamtx"
fi

printf 'Media directory successfully staged: %s\n' "$output_dir"
