"""Tests for upstream dependency tracking and transitive blocking."""
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
OTHER = b"tampered-symbols!!" * 64


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


def submit(client, key, build, upstreams=None, image=IMAGE, symbols=SYMBOLS):
    put_file(client, key, build, "image", image)
    put_file(client, key, build, "symbols", symbols)
    if upstreams is None:
        return client.post(f"/v1/keys/{key}/builds/{build}/commit")
    return client.post(
        f"/v1/keys/{key}/builds/{build}/commit",
        json={"upstreams": upstreams},
    )


def make_hit(client, key, upstreams=None):
    """Two agreeing builders -> key becomes a hit."""
    for builder in ("builder-1", "builder-2"):
        r = submit(client, key, builder, upstreams=upstreams)
        assert r.status_code == 200, r.text
    assert client.get(f"/v1/keys/{key}").json()["status"] == "hit"


# ------------------------------------------------------------- fixing the set


def test_first_commit_fixes_upstream_set(client):
    make_hit(client, "up-a")
    make_hit(client, "up-b")

    r = submit(client, "child", "builder-1", upstreams=["up-a", "up-b"])
    assert r.status_code == 200
    body = r.json()
    assert body["upstreams"] == ["up-a", "up-b"]
    assert body["key_status"]["upstreams"] == ["up-a", "up-b"]

    # second machine must report the identical set
    r = submit(client, "child", "builder-2", upstreams=["up-a", "up-b"])
    assert r.status_code == 200

    status = client.get("/v1/keys/child").json()
    assert status["status"] == "hit"
    assert status["upstreams"] == ["up-a", "up-b"]


def test_omitted_list_means_empty_set(client):
    # no body at all
    r = submit(client, "k1", "builder-1")
    assert r.status_code == 200
    assert r.json()["upstreams"] == []
    # explicit empty body behaves the same (files must still be staged first)
    put_file(client, "k1", "builder-2", "image", IMAGE)
    put_file(client, "k1", "builder-2", "symbols", SYMBOLS)
    r = client.post("/v1/keys/k1/builds/builder-2/commit", json={})
    assert r.status_code == 200
    assert r.json()["upstreams"] == []
    assert client.get("/v1/keys/k1").json()["status"] == "hit"


def test_diverging_sets_between_machines_rejected_and_retained(client):
    make_hit(client, "up-a")
    make_hit(client, "up-b")

    r = submit(client, "child", "builder-1", upstreams=["up-a"])
    assert r.status_code == 200

    # second machine reports a different set -> rejected, records retained
    r = submit(client, "child", "builder-2", upstreams=["up-a", "up-b"])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "upstream_set_mismatch"
    assert detail["fixed_upstreams"] == ["up-a"]
    assert detail["reported_upstreams"] == ["up-a", "up-b"]

    # the rejected machine's commit did not take: key still a one-machine miss
    status = client.get("/v1/keys/child").json()
    assert status["status"] == "miss"
    assert status["committed_builds"] == 1
    assert status["upstreams"] == ["up-a"]

    # retrying with the fixed set succeeds
    r = submit(client, "child", "builder-2", upstreams=["up-a"])
    assert r.status_code == 200
    assert client.get("/v1/keys/child").json()["status"] == "hit"


def test_empty_set_then_nonempty_rejected(client):
    make_hit(client, "up-a")
    # first commit fixes the EMPTY set (omitted list)
    r = submit(client, "child", "builder-1")
    assert r.status_code == 200
    r = submit(client, "child", "builder-2", upstreams=["up-a"])
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "upstream_set_mismatch"


def test_nonempty_fixed_set_then_omitted_list_rejected(client):
    make_hit(client, "up-a")
    r = submit(client, "child", "builder-1", upstreams=["up-a"])
    assert r.status_code == 200
    # old-style request (no list) == empty set, which differs from fixed set
    r = submit(client, "child", "builder-2")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "upstream_set_mismatch"


def test_upstream_must_be_hit_at_first_commit(client):
    make_hit(client, "up-a")
    # up-b exists but never reached quorum
    submit(client, "up-b", "builder-1")

    r = submit(client, "child", "builder-1", upstreams=["up-a", "up-b"])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "upstream_not_hit"
    assert detail["upstream"] == "up-b"
    assert detail["upstream_status"] == "miss"

    # nothing persisted: child still unknown, no dependency edges
    r = client.get("/v1/keys/child")
    assert r.json()["status"] == "miss"
    assert r.json()["upstreams"] == []

    # unknown upstream is likewise rejected
    r = submit(client, "child", "builder-1", upstreams=["ghost"])
    assert r.status_code == 409
    assert r.json()["detail"]["upstream_status"] == "miss"

    # once up-b reaches quorum, the same commit works
    submit(client, "up-b", "builder-2")
    r = submit(
        client, "child", "builder-1", upstreams=["up-a", "up-b"]
    )
    assert r.status_code == 200


def test_rejected_commit_keeps_staged_files(client):
    make_hit(client, "up-a")
    make_hit(client, "up-b")
    submit(client, "child", "builder-1", upstreams=["up-a"])
    # stage both files on builder-2, then a rejected commit
    r = submit(client, "child", "builder-2", upstreams=["up-b"])
    assert r.status_code == 409
    detail = client.get("/v1/keys/child/builds/builder-2").json()
    assert detail["committed"] is False
    assert set(detail["files"]) == {"image", "symbols"}
    # correcting the set commits the already-staged bytes without re-upload
    r = client.post(
        "/v1/keys/child/builds/builder-2/commit",
        json={"upstreams": ["up-a"]},
    )
    assert r.status_code == 200


# --------------------------------------------------- structural input checks


def test_more_than_eight_upstreams_rejected(client):
    ups = [f"up-{i}" for i in range(9)]
    r = submit(client, "k1", "builder-1", upstreams=ups)
    assert r.status_code == 422
    assert "limit is 8" in r.json()["detail"]
    # exactly eight is accepted structurally (upstreams don't exist -> 409)
    r = submit(client, "k1", "builder-1", upstreams=ups[:8])
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "upstream_not_hit"


def test_duplicate_and_self_upstreams_rejected(client):
    r = submit(client, "k1", "builder-1", upstreams=["up-a", "up-a"])
    assert r.status_code == 422
    assert "unique" in r.json()["detail"]

    r = submit(client, "k1", "builder-1", upstreams=["k1"])
    assert r.status_code == 422
    assert "itself" in r.json()["detail"]


def test_invalid_upstream_name_rejected(client):
    r = submit(client, "k1", "builder-1", upstreams=["bad name!"])
    assert r.status_code == 422


# ---------------------------------------------------- transitive deactivation


def test_disabled_upstream_blocks_hits_and_reports_chain(client):
    make_hit(client, "up-a")
    make_hit(client, "mid", upstreams=["up-a"])

    status = client.get("/v1/keys/mid").json()
    assert status["status"] == "hit"
    assert client.get("/v1/keys/mid/files/image").status_code == 200

    # up-a becomes disabled through a diverging third commit
    submit(client, "up-a", "builder-3", symbols=OTHER)
    assert client.get("/v1/keys/up-a").json()["status"] == "disabled"

    status = client.get("/v1/keys/mid").json()
    assert status["status"] == "blocked"
    assert status["action"] == "rebuild"
    assert status["dependency_chain"] == ["mid", "up-a"]
    assert status["root_cause"]["input_key"] == "up-a"
    assert status["root_cause"]["differences"][0]["file"] == "symbols"

    r = client.get("/v1/keys/mid/files/image")
    assert r.status_code == 409
    body = r.json()
    assert body["status"] == "blocked"
    assert body["dependency_chain"] == ["mid", "up-a"]


def test_two_level_dependency_chain(client):
    make_hit(client, "root")
    make_hit(client, "level1", upstreams=["root"])
    make_hit(client, "level2", upstreams=["level1"])

    # all three serve bytes initially
    for k in ("root", "level1", "level2"):
        assert client.get(f"/v1/keys/{k}").json()["status"] == "hit"

    submit(client, "root", "builder-3", symbols=OTHER)

    status = client.get("/v1/keys/level2").json()
    assert status["status"] == "blocked"
    assert status["dependency_chain"] == ["level2", "level1", "root"]
    assert status["root_cause"]["input_key"] == "root"

    r = client.get("/v1/keys/level2/files/symbols")
    assert r.status_code == 409
    assert r.json()["dependency_chain"] == ["level2", "level1", "root"]

    # level1 is blocked one hop earlier
    status = client.get("/v1/keys/level1").json()
    assert status["status"] == "blocked"
    assert status["dependency_chain"] == ["level1", "root"]


def test_unaffected_keys_keep_working(client):
    make_hit(client, "up-a")
    make_hit(client, "up-b")
    make_hit(client, "child-a", upstreams=["up-a"])
    make_hit(client, "solo")

    submit(client, "up-a", "builder-3", symbols=OTHER)

    # child-a is blocked; everything else stays fully usable
    assert client.get("/v1/keys/child-a").json()["status"] == "blocked"
    for k in ("up-b", "solo"):
        status = client.get(f"/v1/keys/{k}").json()
        assert status["status"] == "hit"
        r = client.get(f"/v1/keys/{k}/files/image")
        assert r.status_code == 200 and r.content == IMAGE


def test_blocked_upstream_cannot_be_consumed_by_new_key(client):
    make_hit(client, "up-a")
    make_hit(client, "mid", upstreams=["up-a"])
    submit(client, "up-a", "builder-3", symbols=OTHER)

    # a brand-new key cannot pin a blocked upstream at first commit
    r = submit(client, "late-child", "builder-1", upstreams=["mid"])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "upstream_not_hit"
    assert detail["upstream_status"] == "blocked"
    assert client.get("/v1/keys/late-child").json()["upstreams"] == []


def test_upstream_deactivated_between_machine_commits_rejects_second(client):
    make_hit(client, "up-a")
    # machine 1 commits while the upstream is still a hit
    r = submit(client, "child", "builder-1", upstreams=["up-a"])
    assert r.status_code == 200
    # machine 2 stages its build, then the upstream gets disabled
    put_file(client, "child", "builder-2", "image", IMAGE)
    put_file(client, "child", "builder-2", "symbols", SYMBOLS)
    submit(client, "up-a", "builder-3", symbols=OTHER)
    assert client.get("/v1/keys/up-a").json()["status"] == "disabled"

    # machine 2 reports the correct fixed set, but the upstream no longer
    # hits -> rejected, original records and staged bytes retained
    r = client.post(
        "/v1/keys/child/builds/builder-2/commit",
        json={"upstreams": ["up-a"]},
    )
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "upstream_not_hit"
    assert detail["upstream"] == "up-a"
    assert detail["upstream_status"] == "disabled"

    status = client.get("/v1/keys/child").json()
    assert status["committed_builds"] == 1
    assert status["upstreams"] == ["up-a"]
    detail = client.get("/v1/keys/child/builds/builder-2").json()
    assert detail["committed"] is False
    assert set(detail["files"]) == {"image", "symbols"}


def test_chain_persists_across_restart(tmp_path):
    with make_client(tmp_path) as c:
        make_hit(c, "root")
        make_hit(c, "level1", upstreams=["root"])
        make_hit(c, "level2", upstreams=["level1"])
        submit(c, "root", "builder-3", symbols=OTHER)
        assert c.get("/v1/keys/level2").json()["status"] == "blocked"

    # new process over the same data dir: dependency rules still apply
    with make_client(tmp_path) as c2:
        status = c2.get("/v1/keys/level2").json()
        assert status["status"] == "blocked"
        assert status["dependency_chain"] == ["level2", "level1", "root"]
        assert c2.get("/v1/keys/level2/files/image").status_code == 409
        # unrelated keys still commit and fetch normally after restart
        r = submit(c2, "fresh", "builder-1")
        assert r.status_code == 200
        submit(c2, "fresh", "builder-2")
        assert c2.get("/v1/keys/fresh").json()["status"] == "hit"


def test_legacy_key_treated_as_dependency_free(tmp_path):
    """A key committed before the feature exists has no dependencies."""
    with make_client(tmp_path) as c:
        r = submit(c, "legacy", "builder-1")
        assert r.status_code == 200
        r = submit(c, "legacy", "builder-2")
        assert r.status_code == 200

    with make_client(tmp_path) as c2:
        status = c2.get("/v1/keys/legacy").json()
        assert status["status"] == "hit"
        assert status["upstreams"] == []
        # further commits with an omitted (empty) list stay idempotent
        r = c2.post("/v1/keys/legacy/builds/builder-1/commit")
        assert r.status_code == 200
        assert r.json()["upstreams"] == []
        # reporting any non-empty set now conflicts with the empty fixed set
        make_hit(c2, "up-a")
        r = submit(c2, "legacy", "builder-3", upstreams=["up-a"])
        assert r.status_code == 409
        assert r.json()["detail"]["error"] == "upstream_set_mismatch"


def test_idempotent_recommit_ignores_body(client):
    make_hit(client, "up-a")
    make_hit(client, "child", upstreams=["up-a"])
    # recommitting an existing build returns the fixed set, ignores the body
    r = client.post(
        "/v1/keys/child/builds/builder-1/commit", json={"upstreams": ["up-a"]}
    )
    assert r.status_code == 200
    assert r.json()["upstreams"] == ["up-a"]
    # even a mismatched body is harmless for an already committed build
    r = client.post(
        "/v1/keys/child/builds/builder-1/commit", json={"upstreams": []}
    )
    assert r.status_code == 200
    assert r.json()["upstreams"] == ["up-a"]


def test_branching_dependencies_report_path_to_disabled_root(client):
    make_hit(client, "root")
    make_hit(client, "other")
    # mid depends on two upstreams; DFS order follows the stored positions
    make_hit(client, "mid", upstreams=["other", "root"])
    submit(client, "root", "builder-3", symbols=OTHER)
    status = client.get("/v1/keys/mid").json()
    assert status["status"] == "blocked"
    assert status["dependency_chain"] == ["mid", "root"]


# ---------------------------------------------------------------- concurrency


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


def test_concurrent_first_commits_race_on_fixed_set(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        # two candidate upstreams, both hits
        for up in ("up-a", "up-b"):
            for builder in ("builder-1", "builder-2"):
                for kind, data in (("image", IMAGE), ("symbols", SYMBOLS)):
                    assert (
                        c.put(
                            f"/v1/keys/{up}/builds/{builder}/files/{kind}",
                            content=data,
                        ).status_code
                        == 200
                    )
                c.post(f"/v1/keys/{up}/builds/{builder}/commit")
            assert c.get(f"/v1/keys/{up}").json()["status"] == "hit"

        # stage identical builds for two machines of the racing key
        for builder in ("builder-1", "builder-2"):
            for kind, data in (("image", IMAGE), ("symbols", SYMBOLS)):
                c.put(
                    f"/v1/keys/race/builds/{builder}/files/{kind}", content=data
                )

    results: dict[str, int] = {}

    def commit(builder: str, upstreams: list[str]):
        with httpx.Client(base_url=base, timeout=10) as c:
            r = c.post(
                f"/v1/keys/race/builds/{builder}/commit",
                json={"upstreams": upstreams},
            )
            results[builder] = r.status_code

    barrier = threading.Barrier(2)

    def raced(builder, upstreams):
        barrier.wait()
        commit(builder, upstreams)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [
            pool.submit(raced, "builder-1", ["up-a"]),
            pool.submit(raced, "builder-2", ["up-b"]),
        ]
        for f in futs:
            f.result()

    # exactly one set wins: one commit accepted, the other rejected
    assert sorted(results.values()) == [200, 409]
    with httpx.Client(base_url=base, timeout=10) as c:
        status = c.get("/v1/keys/race").json()
        assert status["upstreams"] in (["up-a"], ["up-b"])
        # the losing machine can still commit with the winning set
        loser = next(b for b, code in results.items() if code == 409)
        winning = status["upstreams"]
        r = c.post(
            f"/v1/keys/race/builds/{loser}/commit",
            json={"upstreams": winning},
        )
        assert r.status_code == 200
        assert c.get("/v1/keys/race").json()["status"] == "hit"


def test_concurrent_disable_propagates_to_blocked_fetches(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        # root must reach its quorum before mid can pin it as an upstream
        for builder in ("builder-1", "builder-2"):
            for kind, data in (("image", IMAGE), ("symbols", SYMBOLS)):
                c.put(
                    f"/v1/keys/root/builds/{builder}/files/{kind}", content=data
                )
            r = c.post(f"/v1/keys/root/builds/{builder}/commit")
            assert r.status_code == 200, r.text
        assert c.get("/v1/keys/root").json()["status"] == "hit"
        for builder in ("builder-1", "builder-2"):
            for kind, data in (("image", IMAGE), ("symbols", SYMBOLS)):
                c.put(
                    f"/v1/keys/mid/builds/{builder}/files/{kind}", content=data
                )
            r = c.post(
                f"/v1/keys/mid/builds/{builder}/commit",
                json={"upstreams": ["root"]},
            )
            assert r.status_code == 200, r.text
        assert c.get("/v1/keys/mid").json()["status"] == "hit"

    errors: list[str] = []

    def fetch_loop(stop):
        with httpx.Client(base_url=base, timeout=10) as c:
            while not stop.is_set():
                r = c.get("/v1/keys/mid/files/image")
                if r.status_code == 200:
                    if r.content != IMAGE:
                        errors.append("served wrong bytes")
                elif r.status_code == 409:
                    if r.json()["status"] != "blocked":
                        errors.append("409 without blocked status")
                    if r.json()["dependency_chain"] != ["mid", "root"]:
                        errors.append("wrong dependency chain")
                else:
                    errors.append(f"unexpected {r.status_code}")

    def disable_root():
        with httpx.Client(base_url=base, timeout=10) as c:
            c.put(
                "/v1/keys/root/builds/builder-3/files/image", content=IMAGE
            )
            c.put(
                "/v1/keys/root/builds/builder-3/files/symbols", content=OTHER
            )
            c.post("/v1/keys/root/builds/builder-3/commit")

    stop = threading.Event()
    with ThreadPoolExecutor(max_workers=4) as pool:
        fetchers = [pool.submit(fetch_loop, stop) for _ in range(3)]
        try:
            pool.submit(disable_root).result()
            time.sleep(0.2)
        finally:
            stop.set()
        for f in fetchers:
            f.result()

    assert errors == []
    with httpx.Client(base_url=base, timeout=10) as c:
        body = c.get("/v1/keys/mid").json()
        assert body["status"] == "blocked"
        assert body["dependency_chain"] == ["mid", "root"]
