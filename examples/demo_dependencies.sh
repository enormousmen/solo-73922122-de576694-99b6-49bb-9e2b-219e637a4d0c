#!/usr/bin/env bash
# Demo: upstream dependency tracking across two dependency levels.
#
# Build graph:
#   root  (no dependencies)
#     └── level1  (depends on root)
#           └── level2  (depends on level1)
#
# Each key reaches a hit after two build machines agree. When `root` is
# disabled by a diverging third commit, level1 and level2 stop serving bytes:
# queries return status=blocked with action=rebuild and the full dependency
# chain leading to the disabled key. The script also shows the commit-time
# guards (fixed upstream set, upstream-must-still-hit, 8-key limit).
#
# Usage:  ./examples/demo_dependencies.sh [BASE_URL]
#         (default http://localhost:8080)
set -euo pipefail

BASE="${1:-http://localhost:8080}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pp() {  # pretty-print JSON if a formatter is available
  if command -v jq >/dev/null; then jq; else cat; fi
}
upload() { # key build kind file
  curl -sS -X PUT "$BASE/v1/keys/$1/builds/$2/files/$3" \
    -H 'Content-Type: application/octet-stream' --data-binary @"$4"
}
# commit key build [upstream-json-array-content]
commit() {
  local key="$1" build="$2"
  if [ "$#" -ge 3 ]; then
    curl -sS -X POST "$BASE/v1/keys/$key/builds/$build/commit" \
      -H 'Content-Type: application/json' --data "$3"
  else
    # no body at all -> reports the empty set
    curl -sS -X POST "$BASE/v1/keys/$key/builds/$build/commit"
  fi
}
status() { curl -sS "$BASE/v1/keys/$1"; }
expect_code() { # expected description -- curl args...
  local expected="$1" desc="$2"; shift 2
  local code
  code=$(curl -sS -o "$WORK/resp.json" -w '%{http_code}' "$@")
  if [ "$code" != "$expected" ]; then
    echo "FAIL: $desc: expected HTTP $expected, got $code" >&2
    cat "$WORK/resp.json" >&2
    exit 1
  fi
  echo "ok: $desc (HTTP $code)"
}

# Deterministic payloads per key so the two builders always agree, while the
# tampered payload differs only in the symbols file.
payload() { printf '%s-%s' "$1" "$2"; }

submit_key() { # key [space-separated upstream keys]
  local key="$1" ups="${2:-}" builder body
  if [ -n "$ups" ]; then
    body=$(python3 -c 'import json,sys;print(json.dumps({"upstreams":sys.argv[1:]}))' $ups)
  fi
  for builder in builder-1 builder-2; do
    payload "$key" image   > "$WORK/$key-$builder-image"
    payload "$key" symbols > "$WORK/$key-$builder-symbols"
    upload "$key" "$builder" image   "$WORK/$key-$builder-image"   > /dev/null
    upload "$key" "$builder" symbols "$WORK/$key-$builder-symbols" > /dev/null
    if [ -n "$ups" ]; then
      commit "$key" "$builder" "$body" > /dev/null
    else
      commit "$key" "$builder" > /dev/null
    fi
  done
}

echo "### 1. root key: two machines commit with no upstream list (empty set)"
submit_key root
status root | pp
[ "$(status root | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')" = "hit" ] \
  || { echo "FAIL: root should be a hit" >&2; exit 1; }

echo; echo "### 2. level1 commits with root as its fixed upstream"
submit_key level1 root
status level1 | pp

echo; echo "### 2a. guard: another machine reporting a different set is rejected"
payload level1 image   > "$WORK/l1-b3-image"
payload level1 symbols > "$WORK/l1-b3-symbols"
upload level1 builder-3 image   "$WORK/l1-b3-image"   > /dev/null
upload level1 builder-3 symbols "$WORK/l1-b3-symbols" > /dev/null
# level1 fixed ["root"]; reporting the empty set (omitted-list equivalent)
# conflicts with it -> 409, and the commit record is not created.
expect_code 409 "upstream-set mismatch rejected" \
  -X POST "$BASE/v1/keys/level1/builds/builder-3/commit" \
  -H 'Content-Type: application/json' --data '{"upstreams": []}'
pp < "$WORK/resp.json"
echo "--- original records retained: level1 still has two committed builders"
status level1 | pp

echo; echo "### 2b. guard: a first commit cannot pin an upstream that is not a hit"
payload ghost image   > "$WORK/ghost-image"
payload ghost symbols > "$WORK/ghost-symbols"
upload ghost builder-1 image   "$WORK/ghost-image"   > /dev/null
upload ghost builder-1 symbols "$WORK/ghost-symbols" > /dev/null
expect_code 409 "non-hit upstream rejected" \
  -X POST "$BASE/v1/keys/ghost/builds/builder-1/commit" \
  -H 'Content-Type: application/json' --data '{"upstreams": ["never-seen"]}'
pp < "$WORK/resp.json"

echo; echo "### 2c. guard: more than 8 upstream keys rejected (HTTP 422)"
expect_code 422 "nine upstreams rejected" \
  -X POST "$BASE/v1/keys/ghost/builds/builder-1/commit" \
  -H 'Content-Type: application/json' \
  --data '{"upstreams": ["u0","u1","u2","u3","u4","u5","u6","u7","u8"]}'
pp < "$WORK/resp.json"

echo; echo "### 3. level2 commits with level1 as upstream (two-level chain)"
submit_key level2 level1
status level2 | pp

echo; echo "### 4. all three keys serve confirmed bytes before deactivation"
for key in root level1 level2; do
  curl -sS -o "$WORK/dl-$key-image" "$BASE/v1/keys/$key/files/image"
  payload "$key" image > "$WORK/want-$key-image"
  cmp "$WORK/want-$key-image" "$WORK/dl-$key-image" && echo "$key image OK"
done

echo; echo "### 5. root is disabled: builder-3 commits DIFFERENT symbols"
printf 'TAMPERED-root-symbols' > "$WORK/root-bad-symbols"
payload root image > "$WORK/root-b3-image"
upload root builder-3 image   "$WORK/root-b3-image"      > /dev/null
upload root builder-3 symbols "$WORK/root-bad-symbols"   > /dev/null
commit root builder-3 > /dev/null
echo "--- root status:"
status root | pp

echo; echo "### 6. level1 query: blocked with one-hop rebuild chain"
status level1 | pp

echo; echo "### 7. level2 query: blocked with the TWO-LEVEL chain level2 -> level1 -> root"
status level2 | pp

echo; echo "### 8. downloads of the blocked keys return 409 with the chain"
expect_code 409 "level2 image download refused" \
  "$BASE/v1/keys/level2/files/image"
pp < "$WORK/resp.json"
expect_code 409 "level1 symbols download refused" \
  "$BASE/v1/keys/level1/files/symbols"

echo; echo "### 9. a new key cannot pin the now-blocked level1"
payload late image   > "$WORK/late-image"
payload late symbols > "$WORK/late-symbols"
upload late builder-1 image   "$WORK/late-image"   > /dev/null
upload late builder-1 symbols "$WORK/late-symbols" > /dev/null
expect_code 409 "blocked upstream cannot be consumed" \
  -X POST "$BASE/v1/keys/late/builds/builder-1/commit" \
  -H 'Content-Type: application/json' --data '{"upstreams": ["level1"]}'
pp < "$WORK/resp.json"

echo; echo "### 10. unrelated keys keep committing and fetching"
submit_key unrelated
expect_code 200 "unrelated key still downloads" \
  "$BASE/v1/keys/unrelated/files/image"
echo "all dependency demo checks passed"
