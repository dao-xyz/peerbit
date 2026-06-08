#!/usr/bin/env bash
set -euo pipefail

version="${WASM_PACK_VERSION:-0.13.1}"
install_dir="${WASM_PACK_INSTALL_DIR:-$HOME/.cargo/bin}"

case "$(uname -s)-$(uname -m)" in
	Linux-x86_64)
		target="x86_64-unknown-linux-musl"
		;;
	*)
		echo "Unsupported wasm-pack platform: $(uname -s)-$(uname -m)" >&2
		exit 1
		;;
esac

mkdir -p "$install_dir"
if [ -n "${GITHUB_PATH:-}" ]; then
	echo "$install_dir" >> "$GITHUB_PATH"
fi

if [ -x "$install_dir/wasm-pack" ]; then
	"$install_dir/wasm-pack" --version
	exit 0
fi

archive="wasm-pack-v${version}-${target}.tar.gz"
url="https://github.com/rustwasm/wasm-pack/releases/download/v${version}/${archive}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

curl \
	--fail \
	--location \
	--show-error \
	--retry 5 \
	--retry-all-errors \
	--retry-delay 2 \
	--connect-timeout 20 \
	--max-time 180 \
	"$url" \
	-o "$tmpdir/$archive"

tar -xzf "$tmpdir/$archive" -C "$tmpdir"
cp "$tmpdir/wasm-pack-v${version}-${target}/wasm-pack" "$install_dir/wasm-pack"
chmod +x "$install_dir/wasm-pack"
"$install_dir/wasm-pack" --version
