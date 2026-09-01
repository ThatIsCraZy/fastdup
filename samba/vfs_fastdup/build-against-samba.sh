#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
	echo "usage: $0 SAMBA_SOURCE_TREE" >&2
	exit 2
fi

workspace=${FASTDUP_WORKSPACE:-/source/fastdup}
samba_tree=$1
artifact_root="$workspace/.artifacts/samba-vfs-fastdup"
# Samba's PIDL generators still derive generated include paths from the
# conventional in-tree `bin/default` location even when waf is given an
# external --out directory.  The Samba checkout itself is already a disposable
# workspace-local artifact, so use its supported default output directory.
build_root="$samba_tree/bin"
module_dir="$samba_tree/source3/modules"
wscript="$module_dir/wscript_build"

case "$samba_tree" in
	"$workspace"/.artifacts/*) ;;
	*)
		echo "Samba build tree must be under $workspace/.artifacts" >&2
		exit 2
		;;
esac

test -f "$samba_tree/VERSION"
test -f "$wscript"
mkdir -p "$artifact_root" "$build_root" "$workspace/.artifacts/tmp"

cp "$workspace/samba/vfs_fastdup/vfs_fastdup.c" "$module_dir/vfs_fastdup.c"
cp "$workspace/samba/vfs_fastdup/vfs_fastdup_contract.c" \
	"$module_dir/vfs_fastdup_contract.c"
cp "$workspace/samba/vfs_fastdup/vfs_fastdup_contract.h" \
	"$module_dir/vfs_fastdup_contract.h"

if ! grep -q "SAMBA3_MODULE('vfs_fastdup'" "$wscript"; then
	sed -i '$r '"$workspace/samba/vfs_fastdup/wscript_build.fragment" "$wscript"
fi

cd "$samba_tree"
TMPDIR="$workspace/.artifacts/tmp" ./configure \
	--without-ad-dc \
	--without-ads \
	--without-ldap \
	--without-pam \
	--without-json \
	--without-libarchive \
	--without-ldb-lmdb \
	--bundled-libraries='!talloc,!tdb,!tevent,!ldb' \
	--private-libraries='dcerpc-samr,samba-policy,dcerpc,samba-hostconfig,samba-credentials,dcerpc_server,samdb' \
	--disable-rpath-install \
	--disable-cups \
	--disable-iprint \
	--disable-python \
	--with-shared-modules='vfs_fastdup,!vfs_snapper'

TMPDIR="$workspace/.artifacts/tmp" PYTHONHASHSEED=1 ./buildtools/bin/waf \
	build --targets=vfs_fastdup

module=$(find "$build_root" -type f -name 'libvfs_module_fastdup.so' -print \
	| head -n 1)
test -n "$module"
printf '%s\n' "$module"
