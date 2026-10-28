#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

required_files=(
  "deploy/linux/media-versions.env"
  "deploy/linux/stage-media-tools.sh"
  "deploy/linux/build-relay-link-deb.sh"
  "deploy/linux/setup-service-role.sh"
  "deploy/linux/start-ui.sh"
  "deploy/linux/README.md"
  "deploy/linux/relay-linkd-server.service"
  "deploy/linux/relay-linkd-relay.service"
  "deploy/linux/package/control.in"
  "deploy/linux/package/postinst"
  "deploy/linux/package/prerm"
  "deploy/linux/package/postrm"
  "config/config.example.toml"
  "deploy/windows/installer-resources/config/mediamtx.yml"
)

missing=()
for relative_path in "${required_files[@]}"; do
  if [[ ! -f "$repo_root/$relative_path" ]]; then
    missing+=("$relative_path")
  fi
done

if (( ${#missing[@]} > 0 )); then
  printf 'Missing Linux package files:\n' >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

grep -Fq 'FFMPEG_VERSION=9.0.1' "$repo_root/deploy/linux/media-versions.env"
grep -Fq 'FFMPEG_SOURCE_ARCHIVE_NAME=ffmpeg-9.0.1.tar.xz' "$repo_root/deploy/linux/media-versions.env"
grep -Fq 'MEDIAMTX_VERSION=v1.18.2' "$repo_root/deploy/linux/media-versions.env"
grep -Fq 'ExecStart=/usr/local/bin/relay-linkd server' "$repo_root/deploy/linux/relay-linkd-server.service"
grep -Fq 'ExecStart=/usr/local/bin/relay-linkd relay' "$repo_root/deploy/linux/relay-linkd-relay.service"

printf 'Linux package layout check passed.\n'
