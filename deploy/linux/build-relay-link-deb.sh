#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
package_version="$(awk -F'"' '$1 ~ /^version[[:space:]]*=/ { print $2; exit }' "$repo_root/Cargo.toml")"
package_arch="$(dpkg --print-architecture 2>/dev/null || uname -m)"
case "$package_arch" in
  aarch64) package_arch="arm64" ;;
  x86_64) package_arch="amd64" ;;
esac
output_path="$repo_root/target/linux-package/relay-linkd_${package_version}_${package_arch}.deb"

while (( $# > 0 )); do
  case "$1" in
    --output)
      [[ $# -ge 2 ]] || { echo "--output requires a path" >&2; exit 2; }
      output_path="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'USAGE'
Usage: build-relay-link-deb.sh [--output PATH]

Build the relay-linkd Ubuntu amd64 Debian package and bundle the pinned Linux
FFmpeg and MediaMTX executables.
USAGE
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

case "$output_path" in
  /*) ;;
  *) output_path="$PWD/$output_path" ;;
esac

if ! command -v cargo >/dev/null 2>&1 && [[ -x "${HOME:-}/.cargo/bin/cargo" ]]; then
  export PATH="${HOME:-}/.cargo/bin:$PATH"
fi

for command_name in cargo dpkg-deb install sed awk; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required command not found: $command_name" >&2
    exit 1
  }
done

mkdir -p "$(dirname -- "$output_path")"
# Keep the package root on the WSL filesystem. A /mnt/c temporary root may
# appear as mode 0777 through DrvFS, which dpkg-deb rejects for DEBIAN/.
build_dir="$(mktemp -d "/tmp/relay-link-package.XXXXXX")"
trap 'rm -rf -- "$build_dir"' EXIT
package_root="$build_dir/root"

printf 'Building relay-linkd release binary...\n'
cargo build --release -p relay-linkd --manifest-path "$repo_root/Cargo.toml"

install -Dm0755 "$repo_root/target/release/relay-linkd" \
    "$package_root/usr/local/bin/relay-linkd"
install -Dm0755 "$repo_root/deploy/linux/setup-service-role.sh" \
    "$package_root/usr/local/bin/relay-link-setup"

bash "$repo_root/deploy/linux/stage-media-tools.sh" \
    --output-dir "$package_root/usr/local/bin/media" \
    --cache-dir "$repo_root/target/linux-media-cache"

install -Dm0644 "$repo_root/config/config.example.toml" \
    "$package_root/usr/local/share/relay-link/config.example.toml"
mediamtx_yml_src=""
if [[ -f "$repo_root/deploy/linux/mediamtx.yml" ]]; then
  mediamtx_yml_src="$repo_root/deploy/linux/mediamtx.yml"
elif [[ -f "$repo_root/deploy/windows/installer-resources/config/mediamtx.yml" ]]; then
  mediamtx_yml_src="$repo_root/deploy/windows/installer-resources/config/mediamtx.yml"
elif [[ -f "$repo_root/config/mediamtx.yml" ]]; then
  mediamtx_yml_src="$repo_root/config/mediamtx.yml"
else
  echo "[-] mediamtx.yml 配置文件未找到！" >&2
  exit 1
fi

install -Dm0644 "$mediamtx_yml_src" \
    "$package_root/usr/local/share/relay-link/mediamtx.yml"
install -Dm0644 "$repo_root/deploy/linux/media-versions.env" \
    "$package_root/usr/local/share/relay-link/media-versions.env"
install -Dm0644 "$repo_root/deploy/linux/README.md" \
    "$package_root/usr/local/share/relay-link/README.md"
install -Dm0644 "$repo_root/deploy/linux/relay-linkd-server.service" \
    "$package_root/lib/systemd/system/relay-linkd-server.service"
install -Dm0644 "$repo_root/deploy/linux/relay-linkd-relay.service" \
    "$package_root/lib/systemd/system/relay-linkd-relay.service"

install -d -m 0755 "$package_root/DEBIAN"
sed -e "s/@VERSION@/$package_version/g" \
    -e "s/@ARCH@/$package_arch/g" \
    "$repo_root/deploy/linux/package/control.in" \
    > "$package_root/DEBIAN/control"
install -m 0755 "$repo_root/deploy/linux/package/postinst" "$package_root/DEBIAN/postinst"
install -m 0755 "$repo_root/deploy/linux/package/prerm" "$package_root/DEBIAN/prerm"
install -m 0755 "$repo_root/deploy/linux/package/postrm" "$package_root/DEBIAN/postrm"

package_tmp="$output_path.tmp.$$"
rm -f -- "$package_tmp"
dpkg-deb --build --root-owner-group "$package_root" "$package_tmp" >/dev/null
mv -f -- "$package_tmp" "$output_path"

dist_dir="$repo_root/dist-linux"
mkdir -p "$dist_dir"
cp -f "$output_path" "$dist_dir/"

printf 'Debian package: %s\n' "$output_path"
printf 'Output in dist-linux: %s/%s\n' "$dist_dir" "$(basename "$output_path")"
printf 'Package version: %s\n' "$package_version"
