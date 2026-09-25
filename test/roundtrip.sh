#!/bin/sh
set -euo pipefail

cd "$(dirname "$0")/.."

docker build -t cubist-roundtrip . >/dev/null
exec docker run --rm cubist-roundtrip
