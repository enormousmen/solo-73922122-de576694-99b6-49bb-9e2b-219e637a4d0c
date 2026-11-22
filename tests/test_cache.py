"""End-to-end tests for the firmware cache API."""
from __future__ import annotations

import hashlib
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

IMAGE = b"firmware-image-bytes" * 64
SYMBOLS = b"debug-symbols-bytes" * 64
OTHER_SYMBOLS = b"tampered-symbols!!" * 64


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_client(tmp_path, **overrides) -> TestClient:
    settings = Settings(data_dir=str(tmp_path / "data"), **overrides)
    return TestClient(create_app(settings))


@pytest.fixture
def client(tmp_path):
    with make_client(tmp_path) as c:
        yield c


def put_file(client, key, build, kind, content):
    return client.put(
        f"/v1/keys/{key}/builds/{build}/files/{kind}", content=content
    )


def submit(client, key, build, image=IMAGE, symbols=SYMBOLS):
    r1 = put_file(client, key, build, "image", image)
    r2 = put_file(client, key, build, "symbols", symbols)
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    return client.post(f"/v1/keys/{key}/builds/{build}/commit")


# --------------------------------------------------------------------- quorum


def test_miss_until_two_distinct_machines_agree(client):
    assert client.get("/v1/keys/k1").json()["status"] == "miss"

    r = submit(client, "k1", "builder-1")
    assert r.status_code == 200
    body = client.get("/v1/keys/k1").json()
    assert body["status"] == "miss"
    assert body["committed_builds"] == 1
    assert body["needed"] == 1

    submit(client, "k1", "builder-2")
    body = client.get("/v1/keys/k1").json()
    assert body["status"] == "hit"
    assert body["confirmed_by"] == ["builder-1", "builder-2"]
    assert body["files"]["image"]["sha256"] == sha256(IMAGE)
    assert body["files"]["symbols"]["sha256"] == sha256(SYMBOLS)


def test_hit_download_returns_exact_bytes(client):
    submit(client, "k1", "builder-1")
    submit(client, "k1", "builder-2")

    for kind, payload in (("image", IMAGE), ("symbols", SYMBOLS)):
        r = client.get(f"/v1/keys/k1/files/{kind}")
        assert r.status_code == 200
        assert r.content == payload
        assert r.headers["X-SHA256"] == sha256(payload)
        assert r.headers["ETag"] == sha256(payload)


def test_same_machine_reupload_counts_once(client):
    put_file(client, "k1", "builder-1", "image", b"stale")
    put_file(client, "k1", "builder-1", "image", IMAGE)  # replace staged file
    put_file(client, "k1", "builder-1", "symbols", SYMBOLS)
    assert client.post("/v1/keys/k1/builds/builder-1/commit").status_code == 200

    body = client.get("/v1/keys/k1").json()
    assert body["status"] == "miss"  # still only one machine
    assert body["committed_builds"] == 1

    detail = client.get("/v1/keys/k1/builds/builder-1").json()
    assert detail["files"]["image"]["sha256"] == sha256(IMAGE)  # latest wins


# ----------------------------------------------------------------- immutables


def test_committed_build_is_immutable(client):
    submit(client, "k1", "builder-1")

    r = put_file(client, "k1", "builder-1", "image", b"new-bytes")
    assert r.status_code == 409

    # recommit is idempotent and reports the original digests
    r = client.post("/v1/keys/k1/builds/builder-1/commit")
    assert r.status_code == 200
    assert r.json()["files"]["image"]["sha256"] == sha256(IMAGE)


def test_commit_requires_all_files(client):
    put_file(client, "k1", "builder-1", "image", IMAGE)
    r = client.post("/v1/keys/k1/builds/builder-1/commit")
    assert r.status_code == 409
    assert r.json()["detail"]["missing"] == ["symbols"]

    r = client.post("/v1/keys/k1/builds/ghost/commit")
    assert r.status_code == 409
    assert sorted(r.json()["detail"]["missing"]) == ["image", "symbols"]


def test_uncommitted_uploads_do_not_affect_hit(client):
    submit(client, "k1", "builder-1")
    submit(client, "k1", "builder-2")
    assert client.get("/v1/keys/k1").json()["status"] == "hit"

    # third machine uploads in parts, never commits
    put_file(client, "k1", "builder-3", "image", b"completely-different")
    assert client.get("/v1/keys/k1").json()["status"] == "hit"
    r = client.get("/v1/keys/k1/files/image")
    assert r.status_code == 200 and r.content == IMAGE

    put_file(client, "k1", "builder-3", "symbols", OTHER_SYMBOLS)
    assert client.get("/v1/keys/k1").json()["status"] == "hit"


# ------------------------------------------------------------------ mismatch


def test_mismatch_disables_key_and_reports_digests(client):
    submit(client, "k1", "builder-1")
    submit(client, "k1", "builder-2")
    assert client.get("/v1/keys/k1").json()["status"] == "hit"

    # third machine commits a different symbols file -> key disabled
    r = submit(client, "k1", "builder-3", symbols=OTHER_SYMBOLS)
    assert r.json()["key_status"]["status"] == "disabled"

    body = client.get("/v1/keys/k1").json()
    assert body["status"] == "disabled"
    assert body["action"] == "rebuild"
    assert body["differences"] == [
        {
            "file": "symbols",
            "builds": [
                {"build": "builder-1", "sha256": sha256(SYMBOLS)},
                {"build": "builder-3", "sha256": sha256(OTHER_SYMBOLS)},
            ],
        }
    ]

    r = client.get("/v1/keys/k1/files/symbols")
    assert r.status_code == 409
    assert r.json()["status"] == "disabled"


def test_first_two_commits_differ_disables_immediately(client):
    submit(client, "k1", "builder-1", symbols=SYMBOLS)
    submit(client, "k1", "builder-2", symbols=OTHER_SYMBOLS)
    body = client.get("/v1/keys/k1").json()
    assert body["status"] == "disabled"
    assert body["differences"][0]["file"] == "symbols"


def test_disabled_state_survives_restart(tmp_path):
    with make_client(tmp_path) as c:
        submit(c, "k1", "builder-1", symbols=SYMBOLS)
        submit(c, "k1", "builder-2", symbols=OTHER_SYMBOLS)
        assert c.get("/v1/keys/k1").json()["status"] == "disabled"

    # brand-new app instance over the same data dir == service restart
    with make_client(tmp_path) as c2:
        body = c2.get("/v1/keys/k1").json()
        assert body["status"] == "disabled"
        assert body["differences"][0]["builds"][1]["sha256"] == sha256(
            OTHER_SYMBOLS
        )
        assert c2.get("/v1/keys/k1/files/image").status_code == 409


def test_mismatch_in_image_file_reported(client):
    submit(client, "k1", "builder-1")
    submit(client, "k1", "builder-2", image=b"different-image")
    diffs = client.get("/v1/keys/k1").json()["differences"]
    assert [d["file"] for d in diffs] == ["image"]


# -------------------------------------------------------------------- limits


def test_file_size_limit(tmp_path):
    with make_client(tmp_path, max_file_bytes=16) as c:
        assert put_file(c, "k1", "b1", "image", b"x" * 16).status_code == 200
        r = put_file(c, "k1", "b1", "image", b"x" * 17)
        assert r.status_code == 413
        # staged file from the first upload is still intact
        detail = c.get("/v1/keys/k1/builds/b1").json()
        assert detail["files"]["image"]["size"] == 16


def test_oversize_content_length_rejected_early(tmp_path):
    with make_client(tmp_path, max_file_bytes=8) as c:
        r = c.put(
            "/v1/keys/k1/builds/b1/files/image",
            content=b"x" * 100,
            headers={"Content-Length": "100"},
        )
        assert r.status_code == 413


# ----------------------------------------------------------------- validation


def test_invalid_names_and_kinds(client):
    assert put_file(client, "bad key!", "b1", "image", b"x").status_code == 422
    assert put_file(client, "k1", "b1", "kernel", b"x").status_code == 400
    assert client.get("/v1/keys/k1/files/kernel").status_code == 400
    assert client.get("/v1/keys/k1/builds/nope").status_code == 404


def test_keys_are_independent(client):
    submit(client, "k1", "builder-1")
    submit(client, "k1", "builder-2")
    assert client.get("/v1/keys/k1").json()["status"] == "hit"
    assert client.get("/v1/keys/k2").json()["status"] == "miss"


# ----------------------------------------------------------------- concurrency


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def live_server(tmp_path):
    settings = Settings(data_dir=str(tmp_path / "data"))
    port = _free_port()
    config = uvicorn.Config(
        create_app(settings), host="127.0.0.1", port=port, log_level="error"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.5)
            break
        except httpx.TransportError:
            time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def test_concurrent_commits_and_fetches(live_server):
    keys = [f"key-{i}" for i in range(6)]
    payloads = {k: (f"img-{k}".encode(), f"sym-{k}".encode()) for k in keys}
    errors: list[str] = []

    def do_submit(key, build):
        img, sym = payloads[key]
        with httpx.Client(base_url=live_server, timeout=10) as c:
            for kind, data in (("image", img), ("symbols", sym)):
                r = c.put(f"/v1/keys/{key}/builds/{build}/files/{kind}", content=data)
                assert r.status_code == 200, r.text
            r = c.post(f"/v1/keys/{key}/builds/{build}/commit")
            assert r.status_code == 200, r.text

    def fetch_loop(key, stop):
        img, sym = payloads[key]
        with httpx.Client(base_url=live_server, timeout=10) as c:
            while not stop.is_set():
                r = c.get(f"/v1/keys/{key}/files/image")
                if r.status_code == 200:
                    if r.content != img:
                        errors.append(f"{key}: served wrong bytes")
                elif r.status_code != 404:
                    errors.append(f"{key}: unexpected {r.status_code}")

    stop = threading.Event()
    with ThreadPoolExecutor(max_workers=18) as pool:
        fetchers = [pool.submit(fetch_loop, k, stop) for k in keys]
        commits = [
            pool.submit(do_submit, k, b)
            for k in keys
            for b in ("builder-1", "builder-2")
        ]
        try:
            for f in commits:
                f.result()
        finally:
            stop.set()
        for f in fetchers:
            f.result()

    assert errors == []
    with httpx.Client(base_url=live_server) as c:
        for k in keys:
            assert c.get(f"/v1/keys/{k}").json()["status"] == "hit"


def test_concurrent_mismatch_yields_only_hit_or_disabled(live_server):
    key = "race-key"
    with httpx.Client(base_url=live_server, timeout=10) as c:
        for b in ("builder-1", "builder-2"):
            for kind, data in (("image", IMAGE), ("symbols", SYMBOLS)):
                c.put(f"/v1/keys/{key}/builds/{b}/files/{kind}", content=data)
            c.post(f"/v1/keys/{key}/builds/{b}/commit")

    errors: list[str] = []

    def fetch_loop(stop):
        with httpx.Client(base_url=live_server, timeout=10) as c:
            while not stop.is_set():
                r = c.get(f"/v1/keys/{key}/files/image")
                if r.status_code == 200:
                    if r.content != IMAGE:
                        errors.append("served non-confirmed bytes")
                elif r.status_code == 409:
                    if r.json()["status"] != "disabled":
                        errors.append("409 without disabled status")
                else:
                    errors.append(f"unexpected {r.status_code}")

    def bad_commit():
        with httpx.Client(base_url=live_server, timeout=10) as c:
            for kind, data in (("image", IMAGE), ("symbols", OTHER_SYMBOLS)):
                c.put(f"/v1/keys/{key}/builds/builder-3/files/{kind}", content=data)
            c.post(f"/v1/keys/{key}/builds/builder-3/commit")

    stop = threading.Event()
    with ThreadPoolExecutor(max_workers=5) as pool:
        fetchers = [pool.submit(fetch_loop, stop) for _ in range(4)]
        try:
            pool.submit(bad_commit).result()
            time.sleep(0.2)
        finally:
            stop.set()
        for f in fetchers:
            f.result()

    assert errors == []
    with httpx.Client(base_url=live_server) as c:
        assert c.get(f"/v1/keys/{key}").json()["status"] == "disabled"
