# firmware-cache

Firmware CI shared cache. CI reuses a build only after **two build machines
(quorum) commit identical image + debug-symbols** for the same input key.
Any divergence between committed results disables the key permanently and
fetches answer with a rebuild hint. Builds that consumed other builds' cached
artifacts report those **upstream input keys at commit time**; when an upstream
is disabled, every key depending on it (directly or transitively) flips to a
rebuild hint as well.

## Run

```bash
docker compose up --build        # serves on http://localhost:8080
```

State lives in the `cache-data` volume (`/data`: SQLite DB + content-addressed
blobs), so disabled keys, fixed upstream sets and artifact bytes survive
restarts. Configuration via env:

| Variable               | Default   | Meaning                              |
|------------------------|-----------|--------------------------------------|
| `CACHE_DATA_DIR`       | `/data`   | where DB and blobs are stored        |
| `CACHE_MAX_FILE_BYTES` | `33554432`| per-file limit (32 MiB)              |
| `CACHE_QUORUM`         | `2`       | machines that must agree for a hit   |

## API

Build machines (one distinct `build_id` per machine):

```
PUT  /v1/keys/{key}/builds/{build}/files/image     # raw bytes, <= 32 MiB
PUT  /v1/keys/{key}/builds/{build}/files/symbols
POST /v1/keys/{key}/builds/{build}/commit          # optional JSON body:
                                                   # {"upstreams": ["key-a", "key-b"]}
```

CI:

```
GET  /v1/keys/{key}                # hit | miss | disabled | blocked (+ details)
GET  /v1/keys/{key}/files/image    # 200 bytes | 404 miss | 409 disabled/blocked
GET  /v1/keys/{key}/files/symbols
```

Interactive docs at `/docs`. See `examples/demo.sh` for the quorum/mismatch
walkthrough and `examples/demo_dependencies.sh` for a two-level dependency
chain (`root → level1 → level2`) including every commit-time guard.

## Semantics

- **Staged uploads**: files land per `(key, build_id, kind)`; re-uploading
  from the same machine replaces the staged bytes and still counts as one
  machine. Uncommitted uploads never influence hits.
- **Commit** requires both files; afterwards the build is immutable (uploads
  → `409`, recommit is idempotent and ignores any request body). The server
  hashes the actual received bytes (SHA-256) while streaming; hashing holds no
  DB transaction, so other keys keep committing/fetching.
- **Hit**: ≥ `quorum` committed builds with identical digests for both files.
  Fetches are evaluated in one transaction, so CI either downloads the
  confirmed artifact set or gets a miss — never a partial/torn state.
- **Mismatch**: if any two committed builds differ, the key is disabled.
  `GET /v1/keys/{key}` then returns `status=disabled`, `action=rebuild`, and
  the differing files with both sides' digests; downloads return `409`.
  The record is persisted in SQLite.

### Upstream dependencies

When a firmware build reuses cached artifacts from other builds, the commit
request reports the upstream input keys it consumed:

- The `upstreams` list holds **at most 8 keys** (unique, valid key names, no
  self-reference); structural violations return `422`.
- **The first successful commit fixes the upstream set.** A request without a
  body (or without the field) reports the empty set. Every build machine's
  first commit for that key must report the same set, and every upstream must
  still be a hit at that moment. Otherwise the commit is rejected with `409`
  (`upstream_set_mismatch` / `upstream_not_hit`) and the whole transaction is
  rolled back: existing records and staged bytes are retained, and the fixed
  set is unchanged.
- Keys that already had commits before this feature existed are treated as
  dependency-free (an empty fixed set).
- **Transitive deactivation**: when a key is disabled for a reproduction
  conflict, every key that depends on it directly or indirectly becomes
  `blocked` — even if its own quorum was already reached. The status response
  and the `409` download response carry `action=rebuild`, the full
  `dependency_chain` from the queried key to the disabled root, and a
  `root_cause` block with that root's digest differences. New commits cannot
  pin a blocked upstream (`upstream_not_hit`, `upstream_status=blocked`).
- Keys outside the affected subtree keep committing and fetching normally.
  The dependency graph is stored in SQLite and re-evaluated on every query, so
  after a restart the same blocking rules apply without any migration.

## Develop & test

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/ -q     # includes live-server concurrency tests
CACHE_DATA_DIR=$(mktemp -d) .venv/bin/uvicorn app.main:app --port 8080 &
./examples/demo.sh http://localhost:8080
./examples/demo_dependencies.sh http://localhost:8080
```
