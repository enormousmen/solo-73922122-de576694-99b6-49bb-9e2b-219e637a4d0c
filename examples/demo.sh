#!/usr/bin/env bash
# Demo: two builders agree -> cache hit; a third uploading in parts does not
# disturb fetches; a diverging commit disables the key and asks for a rebuild.
#
# Usage:  ./examples/demo.sh [BASE_URL]      (default http://localhost:8080)
set -euo pipefail

BASE="${1:-http://localhost:8080}"
KEY="demo-fw-1.2.3"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pp() {  # pretty-print JSON if a formatter is available
  if command -v jq >/dev/null; then jq; else cat; fi
}
upload() { # key build kind file
  curl -sS -X PUT "$BASE/v1/keys/$1/builds/$2/files/$3" \
    -H 'Content-Type: application/octet-stream' --data-binary @"$4"
}
commit() { curl -sS -X POST "$BASE/v1/keys/$1/builds/$2/commit"; }
status() { curl -sS "$BASE/v1/keys/$1"; }

echo "### inputs: one image + one debug-symbols file"
head -c 1048576 /dev/urandom > "$WORK/image.bin"
head -c  262144 /dev/urandom > "$WORK/symbols.bin"

echo; echo "### builder-1 uploads both files and commits"
upload "$KEY" builder-1 image   "$WORK/image.bin";   echo
upload "$KEY" builder-1 symbols "$WORK/symbols.bin"; echo
commit "$KEY" builder-1 | pp
echo "--- key status (only 1 of 2 machines in):"
status "$KEY" | pp

echo; echo "### builder-2 uploads the identical files and commits"
upload "$KEY" builder-2 image   "$WORK/image.bin";   echo
upload "$KEY" builder-2 symbols "$WORK/symbols.bin"; echo
commit "$KEY" builder-2 | pp
echo "--- key status (two machines agree):"
status "$KEY" | pp

echo; echo "### CI downloads the confirmed artifacts and verifies bytes"
curl -sS -o "$WORK/dl-image.bin"   "$BASE/v1/keys/$KEY/files/image"
curl -sS -o "$WORK/dl-symbols.bin" "$BASE/v1/keys/$KEY/files/symbols"
cmp "$WORK/image.bin"   "$WORK/dl-image.bin"   && echo "image OK"
cmp "$WORK/symbols.bin" "$WORK/dl-symbols.bin" && echo "symbols OK"

echo; echo "### builder-3 uploads IN PARTS (staged, not committed)"
head -c 262144 /dev/urandom > "$WORK/other-symbols.bin"
upload "$KEY" builder-3 image "$WORK/image.bin"; echo
echo "--- key status (uncommitted upload must not affect the hit):"
status "$KEY" | pp
curl -sS -o "$WORK/dl2-image.bin" "$BASE/v1/keys/$KEY/files/image"
cmp "$WORK/image.bin" "$WORK/dl2-image.bin" && echo "fetch still works"

echo; echo "### builder-3 commits a DIFFERENT symbols file"
upload "$KEY" builder-3 symbols "$WORK/other-symbols.bin"; echo
commit "$KEY" builder-3 | pp
echo "--- key status (key disabled, rebuild hinted, both digests listed):"
status "$KEY" | pp
echo "--- fetch attempt now:"
code=$(curl -sS -o "$WORK/fetch.json" -w '%{http_code}' "$BASE/v1/keys/$KEY/files/symbols")
pp < "$WORK/fetch.json"
echo "HTTP $code"
