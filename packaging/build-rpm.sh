#!/usr/bin/env bash
set -euo pipefail

workspace=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_root="$workspace/.artifacts"
package_root="$artifact_root/package"
rpmbuild_root="$artifact_root/rpmbuild"
stage_root="$package_root/fastdup-0.6.0"
ui_build_root="$artifact_root/webui-build"
samba_version=4.23.5
samba_archive="$artifact_root/downloads/samba-$samba_version.tar.gz"
samba_tree="$artifact_root/samba-vfs-fastdup/samba-$samba_version"
samba_sha256=593a43ddd0d57902237dfa76888f7b02cb7fc7747111369cb31e126db4836b9f

export CARGO_TARGET_DIR="$artifact_root/target"
export TMPDIR="$artifact_root/tmp"
mkdir -p "$TMPDIR" "$package_root" \
    "$artifact_root/downloads" "$artifact_root/samba-vfs-fastdup" \
    "$rpmbuild_root/BUILD" "$rpmbuild_root/BUILDROOT" \
    "$rpmbuild_root/RPMS" "$rpmbuild_root/SOURCES" \
    "$rpmbuild_root/SPECS" "$rpmbuild_root/SRPMS"

if command -v npm >/dev/null 2>&1; then
    rm -rf "$ui_build_root"
    mkdir -p "$ui_build_root"
    tar -C "$workspace/web/fastdup-ui" \
        --exclude=node_modules --exclude=dist --exclude='*.tsbuildinfo' \
        -cf - . | tar -C "$ui_build_root" -xf -
    (
        cd "$ui_build_root"
        npm ci --cache "$artifact_root/npm-cache"
        npm run build
    )
    # Retain the built assets in the embedded, version-controlled UI surface.
    rm -rf "$workspace/web/fastdup-ui/dist"
    mkdir -p "$workspace/web/fastdup-ui/dist"
    cp -a "$ui_build_root/dist/." "$workspace/web/fastdup-ui/dist/"
elif [ ! -f "$workspace/web/fastdup-ui/dist/index.html" ]; then
    echo "npm is required because web/fastdup-ui/dist is missing" >&2
    exit 1
else
    echo "npm not found; using the checked-in WebUI dist tree" >&2
fi

cargo build --manifest-path "$workspace/Cargo.toml" --locked --release --bins \
    -p fastdup-appliance \
    -p fastdup-control

if [ ! -f "$samba_archive" ]; then
    curl --fail --location --proto '=https' --tlsv1.2 \
        --output "$samba_archive" \
        "https://download.samba.org/pub/samba/stable/samba-$samba_version.tar.gz"
fi
printf '%s  %s\n' "$samba_sha256" "$samba_archive" | sha256sum --check --status
if [ ! -f "$samba_tree/VERSION" ]; then
    tar -C "$artifact_root/samba-vfs-fastdup" -xzf "$samba_archive"
fi
samba_build_log="$artifact_root/samba-vfs-fastdup/build.log"
FASTDUP_WORKSPACE="$workspace" \
    sh "$workspace/samba/vfs_fastdup/build-against-samba.sh" "$samba_tree" \
    | tee "$samba_build_log"
samba_module=$(tail -n 1 "$samba_build_log")
test -f "$samba_module"

rm -rf "$stage_root"
mkdir -p "$stage_root/bin" "$stage_root/samba-vfs"
install -m 0755 "$CARGO_TARGET_DIR/release/fastdup-durable-fuse" "$stage_root/bin/"
install -m 0755 "$CARGO_TARGET_DIR/release/fastdup-maintenance" "$stage_root/bin/"
install -m 0755 "$CARGO_TARGET_DIR/release/fastdup-control" "$stage_root/bin/"
install -m 0755 "$CARGO_TARGET_DIR/release/fastdup-agent" "$stage_root/bin/"
install -m 0755 "$samba_module" "$stage_root/samba-vfs/fastdup.so"
patchelf --set-rpath /usr/lib64/samba "$stage_root/samba-vfs/fastdup.so"
if ldd "$stage_root/samba-vfs/fastdup.so" | grep -q 'not found'; then
    echo "vfs_fastdup has unresolved Rocky Linux Samba dependencies" >&2
    ldd "$stage_root/samba-vfs/fastdup.so" >&2
    exit 1
fi
cp -a "$workspace/packaging/systemd" "$stage_root/"
cp -a "$workspace/packaging/sysusers.d" "$stage_root/"
cp -a "$workspace/packaging/tmpfiles.d" "$stage_root/"
cp -a "$workspace/packaging/sysctl.d" "$stage_root/"
cp -a "$workspace/packaging/fastdup" "$stage_root/"
cp -a "$workspace/packaging/samba" "$stage_root/"
install -m 0644 "$workspace/README.md" "$stage_root/README.md"

tar -C "$package_root" -czf \
    "$rpmbuild_root/SOURCES/fastdup-0.6.0-x86_64.tar.gz" \
    fastdup-0.6.0
install -m 0644 "$workspace/packaging/rpm/fastdup.spec" \
    "$rpmbuild_root/SPECS/fastdup.spec"

rpmbuild -ba \
    --define "_topdir $rpmbuild_root" \
    --define "_tmppath $TMPDIR" \
    "$rpmbuild_root/SPECS/fastdup.spec"

find "$rpmbuild_root/RPMS" -type f -name 'fastdup-0.6.0-*.x86_64.rpm' -print
