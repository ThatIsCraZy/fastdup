#!/usr/bin/env bash
set -euo pipefail
# A plain directory or underlying root disk must never become a backup target.
[[ $(findmnt -n -o FSTYPE --mountpoint /repository) == fuse* ]]
[[ $(findmnt -n -o SOURCE --mountpoint /repository) == fastdup* ]]
