"""Storage layer: SQLite metadata + content-addressed blob store.

Consistency model:
- Uploads are staged per (input_key, build_id, kind). Re-uploading from the
  same build_id replaces the staged bytes and still counts as one machine.
- A build can be committed only once both required files are staged.
  Committed content is immutable: further uploads to that build are rejected.
- Status evaluation and commit run inside a single IMMEDIATE transaction, so
  concurrent commits/fetches observe either the old or the new state, never a
  torn one. Hashing and blob writes happen outside any DB transaction, so
  other keys keep committing/fetching while a large file is processed.
- A key is a cache hit only when >= quorum committed builds (distinct
  build_ids) have identical digests for every required file.
- If any two committed builds for a key differ, the key is permanently
  disabled and the differing files plus both sides' digests are recorded in
  the database, so the rebuild hint survives restarts.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing, contextmanager

REQUIRED_KINDS = ("image", "symbols")

SCHEMA = """
CREATE TABLE IF NOT EXISTS keys (
    input_key       TEXT PRIMARY KEY,
    disabled        INTEGER NOT NULL DEFAULT 0,
    disabled_reason TEXT
);
CREATE TABLE IF NOT EXISTS builds (
    input_key    TEXT NOT NULL,
    build_id     TEXT NOT NULL,
    committed    INTEGER NOT NULL DEFAULT 0,
    committed_at TEXT,
    PRIMARY KEY (input_key, build_id)
);
CREATE TABLE IF NOT EXISTS files (
    input_key  TEXT NOT NULL,
    build_id   TEXT NOT NULL,
    kind       TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    size       INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (input_key, build_id, kind)
);
"""


class StoreError(Exception):
    """Base class for store-level errors."""


class BuildCommittedError(StoreError):
    """Upload attempted on an already committed build."""


class IncompleteBuildError(StoreError):
    """Commit attempted before all required files are staged."""

    def __init__(self, missing: list[str]):
        super().__init__(f"missing files: {', '.join(missing)}")
        self.missing = missing


@contextmanager
def _tx(conn: sqlite3.Connection, immediate: bool = False):
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


class Store:
    def __init__(self, data_dir: str, quorum: int = 2):
        self.data_dir = os.path.abspath(data_dir)
        self.quorum = quorum
        self.blob_dir = os.path.join(self.data_dir, "blobs")
        self.tmp_dir = os.path.join(self.data_dir, "tmp")
        self.db_path = os.path.join(self.data_dir, "cache.db")
        os.makedirs(self.blob_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)  # executescript manages its own txn

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    # ------------------------------------------------------------------ blobs

    def blob_path(self, sha256: str) -> str:
        return os.path.join(self.blob_dir, sha256)

    def adopt_blob(self, tmp_path: str, sha256: str) -> str:
        """Move a fully written temp file into the content-addressed store."""
        final = self.blob_path(sha256)
        if os.path.exists(final):
            os.remove(tmp_path)  # identical content already stored
        else:
            os.replace(tmp_path, final)  # atomic on same filesystem
        return final

    # ---------------------------------------------------------------- uploads

    def stage_file(
        self, key: str, build: str, kind: str, sha256: str, size: int
    ) -> dict:
        with closing(self._connect()) as conn, _tx(conn, immediate=True):
            conn.execute("INSERT OR IGNORE INTO keys(input_key) VALUES (?)", (key,))
            conn.execute(
                "INSERT OR IGNORE INTO builds(input_key, build_id) VALUES (?, ?)",
                (key, build),
            )
            row = conn.execute(
                "SELECT committed FROM builds WHERE input_key=? AND build_id=?",
                (key, build),
            ).fetchone()
            if row["committed"]:
                raise BuildCommittedError(
                    f"build {build!r} for key {key!r} is already committed"
                )
            conn.execute(
                """
                INSERT INTO files(input_key, build_id, kind, sha256, size, updated_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(input_key, build_id, kind) DO UPDATE SET
                    sha256=excluded.sha256,
                    size=excluded.size,
                    updated_at=excluded.updated_at
                """,
                (key, build, kind, sha256, size),
            )
        return {
            "input_key": key,
            "build_id": build,
            "kind": kind,
            "sha256": sha256,
            "size": size,
            "staged": True,
        }

    # ----------------------------------------------------------------- commit

    def commit_build(self, key: str, build: str) -> dict:
        with closing(self._connect()) as conn, _tx(conn, immediate=True):
            row = conn.execute(
                "SELECT committed FROM builds WHERE input_key=? AND build_id=?",
                (key, build),
            ).fetchone()
            if row is None:
                raise IncompleteBuildError(list(REQUIRED_KINDS))
            files = self._build_files(conn, key, build)
            if not row["committed"]:
                missing = [k for k in REQUIRED_KINDS if k not in files]
                if missing:
                    raise IncompleteBuildError(missing)
                conn.execute(
                    "UPDATE builds SET committed=1, committed_at=datetime('now') "
                    "WHERE input_key=? AND build_id=?",
                    (key, build),
                )
                diffs = self._detect_mismatch(conn, key)
                if diffs is not None:
                    conn.execute(
                        "UPDATE keys SET disabled=1, disabled_reason=? "
                        "WHERE input_key=?",
                        (json.dumps(diffs), key),
                    )
            return {
                "input_key": key,
                "build_id": build,
                "committed": True,
                "files": files,
                "key_status": self._key_status(conn, key),
            }

    # ----------------------------------------------------------------- status

    def key_status(self, key: str) -> dict:
        with closing(self._connect()) as conn, _tx(conn):
            return self._key_status(conn, key)

    def build_detail(self, key: str, build: str) -> dict | None:
        with closing(self._connect()) as conn, _tx(conn):
            row = conn.execute(
                "SELECT committed, committed_at FROM builds "
                "WHERE input_key=? AND build_id=?",
                (key, build),
            ).fetchone()
            if row is None:
                return None
            files = self._build_files(conn, key, build)
            return {
                "input_key": key,
                "build_id": build,
                "committed": bool(row["committed"]),
                "committed_at": row["committed_at"],
                "files": files,
                "missing": [k for k in REQUIRED_KINDS if k not in files],
            }

    def hit_file(self, key: str, kind: str) -> tuple[dict, str | None]:
        """Return (status, blob_path). blob_path is set only on a hit."""
        with closing(self._connect()) as conn, _tx(conn):
            status = self._key_status(conn, key)
            if status["status"] != "hit":
                return status, None
            sha = status["files"][kind]["sha256"]
            return status, self.blob_path(sha)

    # ----------------------------------------------------------------- intern

    def _build_files(self, conn, key: str, build: str) -> dict:
        rows = conn.execute(
            "SELECT kind, sha256, size FROM files "
            "WHERE input_key=? AND build_id=? ORDER BY kind",
            (key, build),
        ).fetchall()
        return {r["kind"]: {"sha256": r["sha256"], "size": r["size"]} for r in rows}

    def _committed_digests(self, conn, key: str) -> dict:
        """{build_id: {kind: {sha256, size}}} for committed builds only."""
        rows = conn.execute(
            """
            SELECT b.build_id AS build_id, f.kind AS kind,
                   f.sha256 AS sha256, f.size AS size
            FROM builds b
            JOIN files f ON f.input_key=b.input_key AND f.build_id=b.build_id
            WHERE b.input_key=? AND b.committed=1
            ORDER BY b.build_id
            """,
            (key,),
        ).fetchall()
        builds: dict[str, dict] = {}
        for r in rows:
            builds.setdefault(r["build_id"], {})[r["kind"]] = {
                "sha256": r["sha256"],
                "size": r["size"],
            }
        return builds

    def _detect_mismatch(self, conn, key: str) -> list | None:
        builds = self._committed_digests(conn, key)
        ids = sorted(builds)
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                diffs = []
                for kind in REQUIRED_KINDS:
                    a = builds[ids[i]][kind]["sha256"]
                    b = builds[ids[j]][kind]["sha256"]
                    if a != b:
                        diffs.append(
                            {
                                "file": kind,
                                "builds": [
                                    {"build": ids[i], "sha256": a},
                                    {"build": ids[j], "sha256": b},
                                ],
                            }
                        )
                if diffs:
                    return diffs
        return None

    def _key_status(self, conn, key: str) -> dict:
        row = conn.execute(
            "SELECT disabled, disabled_reason FROM keys WHERE input_key=?", (key,)
        ).fetchone()
        committed = self._committed_digests(conn, key)
        status = {
            "input_key": key,
            "quorum": self.quorum,
            "committed_builds": len(committed),
        }
        if row is not None and row["disabled"]:
            status.update(
                status="disabled",
                reason="committed builds differ",
                action="rebuild",
                differences=json.loads(row["disabled_reason"]),
            )
            return status
        ids = sorted(committed)
        if len(ids) >= self.quorum:
            first = committed[ids[0]]
            if all(committed[b] == first for b in ids[1:]):
                status.update(status="hit", files=first, confirmed_by=ids)
                return status
        status.update(
            status="miss",
            needed=max(0, self.quorum - len(ids)),
        )
        return status
