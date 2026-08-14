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

Download and stage the pinned Linux amd64 FFmpeg and MediaMTX executables.
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

for command_name in curl sha256sum tar find install gcc make pkg-config nproc; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required command not found: $command_name" >&2
    exit 1
  }
done

verify_archive() {
  local expected_sha256="$1"
  local archive_path="$2"
  printf '%s  %s\n' "$expected_sha256" "$archive_path" | sha256sum --check --status -
}

download_archive() {
  local archive_name="$1"
  local archive_url="$2"
  local expected_sha256="$3"
  local archive_path="$cache_dir/$archive_name"
  local partial_path="$archive_path.partial.$$"

  mkdir -p "$cache_dir"
  if [[ -f "$archive_path" ]] && verify_archive "$expected_sha256" "$archive_path"; then
    printf 'Using verified cache: %s\n' "$archive_path"
    printf '%s\n' "$archive_path"
    return 0
  fi

  rm -f -- "$partial_path"
  printf 'Downloading: %s\n' "$archive_url"
  curl --fail --location --retry 3 --connect-timeout 20 --output "$partial_path" "$archive_url"
  if ! verify_archive "$expected_sha256" "$partial_path"; then
    rm -f -- "$partial_path"
    echo "checksum verification failed: $archive_name" >&2
    exit 1
  fi
  mv -f -- "$partial_path" "$archive_path"
  printf 'Cached verified archive: %s\n' "$archive_path"
  printf '%s\n' "$archive_path"
}

work_dir="$(mktemp -d "/tmp/relay-link-media.XXXXXX")"
ffmpeg_build_dir=""
cleanup() {
  rm -rf -- "$work_dir"
  if [[ -n "$ffmpeg_build_dir" ]]; then
    rm -rf -- "$ffmpeg_build_dir"
  fi
}
trap cleanup EXIT

ffmpeg_archive="$(download_archive "$FFMPEG_SOURCE_ARCHIVE_NAME" "$FFMPEG_SOURCE_ARCHIVE_URL" "$FFMPEG_SOURCE_ARCHIVE_SHA256" | tail -n 1)"
mediamtx_archive="$(download_archive "$MEDIAMTX_ARCHIVE_NAME" "$MEDIAMTX_ARCHIVE_URL" "$MEDIAMTX_ARCHIVE_SHA256" | tail -n 1)"

mkdir -p "$work_dir/ffmpeg" "$work_dir/mediamtx"
tar -xJf "$ffmpeg_archive" -C "$work_dir/ffmpeg"
tar -xzf "$mediamtx_archive" -C "$work_dir/mediamtx"

ffmpeg_cache_dir="$cache_dir/ffmpeg-$FFMPEG_VERSION"
ffmpeg_binary_cache="$ffmpeg_cache_dir/ffmpeg"
if [[ ! -x "$ffmpeg_binary_cache" ]]; then
  ffmpeg_source_dir="$(find "$work_dir/ffmpeg" -mindepth 1 -maxdepth 1 -type d -name "ffmpeg-$FFMPEG_VERSION" -print -quit)"
  [[ -n "$ffmpeg_source_dir" ]] || { echo "FFmpeg source directory not found in archive" >&2; exit 1; }

  ffmpeg_build_dir="$(mktemp -d "$cache_dir/ffmpeg-build.XXXXXX")"
  ffmpeg_x86asm_flag="--disable-x86asm"
  if command -v nasm >/dev/null 2>&1 || command -v yasm >/dev/null 2>&1; then
    ffmpeg_x86asm_flag="--enable-x86asm"
  fi

  (
    cd "$ffmpeg_source_dir"
    ./configure \
      --prefix="$ffmpeg_build_dir/prefix" \
      --disable-debug \
      --disable-doc \
      --disable-ffplay \
      --disable-ffprobe \
      --disable-autodetect \
      --enable-static \
      --disable-shared \
      "$ffmpeg_x86asm_flag"
    make -j"$(nproc)" ffmpeg
  )
  install -d -m 0755 "$ffmpeg_cache_dir"
  install -m 0755 "$ffmpeg_source_dir/ffmpeg" "$ffmpeg_binary_cache"
  rm -rf -- "$ffmpeg_build_dir"
  ffmpeg_build_dir=""
fi

ffmpeg_binary="$ffmpeg_binary_cache"
mediamtx_binary="$(find "$work_dir/mediamtx" -type f -name mediamtx -print -quit)"
[[ -n "$ffmpeg_binary" ]] || { echo "FFmpeg executable not found in archive" >&2; exit 1; }
[[ -n "$mediamtx_binary" ]] || { echo "MediaMTX executable not found in archive" >&2; exit 1; }

install -d -m 0755 "$output_dir/ffmpeg" "$output_dir/mediamtx"
install -m 0755 "$ffmpeg_binary" "$output_dir/ffmpeg/ffmpeg"
install -m 0755 "$mediamtx_binary" "$output_dir/mediamtx/mediamtx"

ffmpeg_version_line="$("$output_dir/ffmpeg/ffmpeg" -version 2>&1 | sed -n '1p')"
if [[ "$ffmpeg_version_line" != "ffmpeg version $FFMPEG_VERSION"* ]]; then
  echo "unexpected FFmpeg version: $ffmpeg_version_line" >&2
  exit 1
fi

mediamtx_version_line="$("$output_dir/mediamtx/mediamtx" --version 2>&1 | sed -n '1p')"
if [[ "$mediamtx_version_line" != *"$MEDIAMTX_VERSION"* ]]; then
  echo "unexpected MediaMTX version: $mediamtx_version_line" >&2
  exit 1
fi

printf 'Staged FFmpeg: %s\n' "$ffmpeg_version_line"
printf 'Staged MediaMTX: %s\n' "$mediamtx_version_line"
printf 'Media directory: %s\n' "$output_dir"
