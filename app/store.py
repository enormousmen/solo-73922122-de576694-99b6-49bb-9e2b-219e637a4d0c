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

Upstream dependencies:
- A commit may report the upstream input keys whose cached artifacts the
  build consumed (at most ``MAX_UPSTREAMS``). The set reported by the first
  successful commit of a key is fixed in the ``dependencies`` table.
- Every build machine's first commit for that key must report the same set,
  and every upstream must still be a hit at that moment; otherwise the commit
  is rejected and the original records are left untouched (transaction
  rollback). Requests omitting the list report an empty set; keys that already
  had commits before this feature existed are treated as dependency-free.
- When an upstream key is disabled (directly or transitively), dependent keys
  stop serving downloads even if their own quorum was reached: status becomes
  ``blocked`` with a rebuild hint and the dependency chain leading to the
  disabled key. The graph is evaluated on every query from persisted rows, so
  the same rules apply after a restart.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing, contextmanager

REQUIRED_KINDS = ("image", "symbols")

# Maximum number of upstream input keys a single commit may report.
MAX_UPSTREAMS = 8

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
CREATE TABLE IF NOT EXISTS dependencies (
    input_key    TEXT NOT NULL,
    upstream_key TEXT NOT NULL,
    position     INTEGER NOT NULL,
    PRIMARY KEY (input_key, upstream_key)
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


class DependencyRejectedError(StoreError):
    """Commit rejected because of the reported upstream dependency set.

    Carries a structured ``detail`` payload suitable for an HTTP 409 body.
    Nothing is written when this is raised: the surrounding transaction is
    rolled back and all previous records are retained.
    """

    def __init__(self, error: str, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": error, "message": message, **extra}


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

    def commit_build(
        self, key: str, build: str, upstreams: list[str] | None = None
    ) -> dict:
        reported = list(upstreams or [])
        with closing(self._connect()) as conn, _tx(conn, immediate=True):
            row = conn.execute(
                "SELECT committed FROM builds WHERE input_key=? AND build_id=?",
                (key, build),
            ).fetchone()
            if row is None:
                raise IncompleteBuildError(list(REQUIRED_KINDS))
            files = self._build_files(conn, key, build)

            if row["committed"]:
                # Recommit is idempotent: return the original record and keep
                # the fixed upstream set, ignoring any body the caller sent.
                return {
                    "input_key": key,
                    "build_id": build,
                    "committed": True,
                    "files": files,
                    "upstreams": self._fixed_upstreams(conn, key) or [],
                    "key_status": self._effective_status(conn, key),
                }

            fixed = self._fixed_upstreams(conn, key)
            has_committed_build = conn.execute(
                "SELECT 1 FROM builds WHERE input_key=? AND committed=1 LIMIT 1",
                (key,),
            ).fetchone() is not None
            # A key is dependency-free when it has dependency rows for the
            # empty set (nothing to store) or committed builds predating this
            # feature: legacy keys are treated as having no dependencies.
            set_is_fixed = fixed is not None or has_committed_build
            effective_fixed = fixed or []

            if set_is_fixed:
                if set(reported) != set(effective_fixed):
                    raise DependencyRejectedError(
                        "upstream_set_mismatch",
                        "reported upstream set differs from the set fixed by "
                        "the first successful commit",
                        fixed_upstreams=effective_fixed,
                        reported_upstreams=reported,
                    )
                to_verify = effective_fixed
            else:
                to_verify = reported
            # Every build machine's first commit for the key requires every
            # upstream to still be a hit (which also rules out disabled or
            # blocked upstreams), even when the set was fixed earlier.
            for upstream in to_verify:
                upstream_status = self._effective_status(conn, upstream)
                if upstream_status["status"] != "hit":
                    raise DependencyRejectedError(
                        "upstream_not_hit",
                        f"upstream key {upstream!r} is not a cache hit",
                        upstream=upstream,
                        upstream_status=upstream_status["status"],
                    )

            missing = [k for k in REQUIRED_KINDS if k not in files]
            if missing:
                raise IncompleteBuildError(missing)
            conn.execute(
                "UPDATE builds SET committed=1, committed_at=datetime('now') "
                "WHERE input_key=? AND build_id=?",
                (key, build),
            )
            if not set_is_fixed:
                for position, upstream in enumerate(reported):
                    conn.execute(
                        "INSERT INTO dependencies(input_key, upstream_key, "
                        "position) VALUES (?, ?, ?)",
                        (key, upstream, position),
                    )
                effective_fixed = reported
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
                "upstreams": effective_fixed,
                "key_status": self._effective_status(conn, key),
            }

    # ----------------------------------------------------------------- status

    def key_status(self, key: str) -> dict:
        with closing(self._connect()) as conn, _tx(conn):
            return self._effective_status(conn, key)

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
                "upstreams": self._fixed_upstreams(conn, key) or [],
            }

    def hit_file(self, key: str, kind: str) -> tuple[dict, str | None]:
        """Return (status, blob_path). blob_path is set only on a live hit."""
        with closing(self._connect()) as conn, _tx(conn):
            status = self._effective_status(conn, key)
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

    def _fixed_upstreams(self, conn, key: str) -> list[str] | None:
        """Fixed upstream list, or None when the key has never fixed a set."""
        rows = conn.execute(
            "SELECT upstream_key FROM dependencies "
            "WHERE input_key=? ORDER BY position",
            (key,),
        ).fetchall()
        if not rows:
            return None
        return [r["upstream_key"] for r in rows]

    def _find_disabled_chain(
        self, conn, key: str, visited: set[str] | None = None
    ) -> list[str] | None:
        """DFS the dependency graph.

        Returns the chain ``[key, ..., disabled_root]`` when a disabled key is
        reachable through upstream edges, otherwise ``None``. The visited set
        is defensive: a cycle cannot be created through the commit API because
        every upstream must be a hit before the edge is stored.
        """
        if visited is None:
            visited = set()
        if key in visited:
            return None
        visited.add(key)
        krow = conn.execute(
            "SELECT disabled FROM keys WHERE input_key=?", (key,)
        ).fetchone()
        if krow is not None and krow["disabled"]:
            return [key]
        for row in conn.execute(
            "SELECT upstream_key FROM dependencies "
            "WHERE input_key=? ORDER BY position",
            (key,),
        ):
            sub = self._find_disabled_chain(conn, row["upstream_key"], visited)
            if sub is not None:
                return [key] + sub
        return None

    def _base_key_status(self, conn, key: str) -> dict:
        """Status derived from this key's own committed builds."""
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

    def _effective_status(self, conn, key: str) -> dict:
        """Base status plus upstream-aware blocking for disabled ancestors."""
        status = self._base_key_status(conn, key)
        status["upstreams"] = self._fixed_upstreams(conn, key) or []
        # A directly disabled key keeps its own detailed status.
        if status["status"] == "disabled":
            return status
        chain = self._find_disabled_chain(conn, key)
        if chain is None:
            return status
        root = chain[-1]
        root_row = conn.execute(
            "SELECT disabled_reason FROM keys WHERE input_key=?", (root,)
        ).fetchone()
        root_cause = {
            "input_key": root,
            "reason": "committed builds differ",
        }
        if root_row is not None and root_row["disabled_reason"] is not None:
            root_cause["differences"] = json.loads(root_row["disabled_reason"])
        status.update(
            status="blocked",
            action="rebuild",
            reason="an upstream dependency is disabled",
            dependency_chain=chain,
            root_cause=root_cause,
        )
        return status
