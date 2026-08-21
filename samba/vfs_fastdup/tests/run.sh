#!/bin/sh
set -eu

workspace=${FASTDUP_WORKSPACE:-/source/fastdup}
artifact_root="$workspace/.artifacts/samba-vfs-fastdup"
mkdir -p "$artifact_root"

cc -std=c11 -Wall -Wextra -Werror -pedantic \
	-I"$workspace/samba/vfs_fastdup" \
	"$workspace/samba/vfs_fastdup/vfs_fastdup_contract.c" \
	"$workspace/samba/vfs_fastdup/tests/contract_test.c" \
	-o "$artifact_root/contract_test"

"$artifact_root/contract_test"
