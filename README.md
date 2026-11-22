# firmware-cache

Firmware CI shared cache. CI reuses a build only after **two build machines
(quorum) commit identical image + debug-symbols** for the same input key.
Any divergence between committed results disables the key permanently and
fetches answer with a rebuild hint.

## Run

```bash
docker compose up --build        # serves on http://localhost:8080
```

State lives in the `cache-data` volume (`/data`: SQLite DB + content-addressed
blobs), so disabled keys and artifacts survive restarts. Configuration via env:

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
POST /v1/keys/{key}/builds/{build}/commit          # needs both files
```

CI:

```
GET  /v1/keys/{key}                # hit | miss | disabled (+ rebuild details)
GET  /v1/keys/{key}/files/image    # 200 bytes | 404 miss | 409 disabled
GET  /v1/keys/{key}/files/symbols
```

Interactive docs at `/docs`. See `examples/demo.sh` for a full walkthrough.

## Semantics

- **Staged uploads**: files land per `(key, build_id, kind)`; re-uploading
  from the same machine replaces the staged bytes and still counts as one
  machine. Uncommitted uploads never influence hits.
- **Commit** requires both files; afterwards the build is immutable (uploads
  → `409`, recommit is idempotent). The server hashes the actual received
  bytes (SHA-256) while streaming; hashing holds no DB transaction, so other
  keys keep committing/fetching.
- **Hit**: ≥ `quorum` committed builds with identical digests for both files.
  Fetches are evaluated in one transaction, so CI either downloads the
  confirmed artifact set or gets a miss — never a partial/torn state.
- **Mismatch**: if any two committed builds differ, the key is disabled.
  `GET /v1/keys/{key}` then returns `status=disabled`, `action=rebuild`, and
  the differing files with both sides' digests; downloads return `409`.
  The record is persisted in SQLite.

## Develop & test

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/ -q     # includes live-server concurrency tests
CACHE_DATA_DIR=$(mktemp -d) .venv/bin/uvicorn app.main:app --port 8080 &
./examples/demo.sh http://localhost:8080
```
