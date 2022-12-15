#!/usr/bin/env bash
set -euo pipefail
scriptDir=$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)

xcaddy build \
	--with "github.com/floj/caddy-s3-fs=$scriptDir"
