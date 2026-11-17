import json

import pytest

from app.modules.m7_merge import core

MAIN = {"logs": [
    '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}',
    '{"request_id":"r2","timestamp":"2026-11-15T02:05:00Z","service":"pay","status":200}',
]}

SH = {"site": "SH", "logs": [
    '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":500}',
    '{"request_id":"r3","timestamp":"2026-11-15T01:20:00Z","service":"pay","status":500}',
]}

SZ = {"site": "SZ", "logs": [
    '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":404}',
    '{"request_id":"r4","timestamp":"2026-11-15T03:10:00Z","service":"search","status":200}',
]}


def ingest_main(client, payload=MAIN):
    response = client.post("/api/merge/main/ingest", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def make_branch(client, payload, seal=True):
    response = client.post("/api/merge/branches", json=payload)
    assert response.status_code == 200, response.text
    branch_id = response.json()["branch_id"]
    if seal:
        assert client.post(f"/api/merge/branches/{branch_id}/seal").status_code == 200
    return branch_id


def merge(client, branch_ids, version=None):
    version = version if version is not None else client.get("/api/merge/main").json()["version"]
    return client.post("/api/merge",
                      json={"branch_ids": branch_ids, "expected_version": version})


def test_m7_page(client):
    response = client.get("/merge")
    assert response.status_code == 200
    assert "panel" in response.text


def test_ingest_is_idempotent(client):
    first = ingest_main(client)
    second = ingest_main(client)
    assert first["version"] == second["version"] == 1
    assert second["changed"] is False
    assert second["accepted"] == 0
    assert second["duplicates"] == 2


def test_ingest_rejects_conflicting_value(client):
    ingest_main(client)
    response = client.post("/api/merge/main/ingest", json={"logs": [
        '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":500}']})
    assert response.status_code == 422
    assert response.json()["error"] == "DUPLICATE_CONFLICT"
    assert client.get("/api/merge/main").json()["events"][0]["status"] == 200


def test_branch_reads_from_actual_log(client):
    ingest_main(client)
    branch = client.get(f"/api/merge/branches/{make_branch(client, SH, seal=False)}").json()
    assert branch["base_event_count"] == 2
    assert branch["own_event_count"] == 2
    # 自有日志可修订起点快照中的同编号事件
    revised = {item["request_id"]: item for item in branch["events"]}
    assert revised["r1"]["status"] == 500
    assert revised["r3"]["status"] == 500
    assert branch["buckets"] == [
        {"hour": "2026-11-15T01:00:00Z", "service": "pay", "count": 2, "errors": 2},
        {"hour": "2026-11-15T02:00:00Z", "service": "pay", "count": 1, "errors": 0},
    ]


def test_receipt_is_immutable_after_seal(client):
    ingest_main(client)
    branch_id = make_branch(client, SH, seal=False)
    client.post(f"/api/merge/branches/{branch_id}/seal")
    receipt = client.get(f"/api/merge/branches/{branch_id}/receipt").json()
    blocked = client.post(f"/api/merge/branches/{branch_id}/logs", json={"logs": []})
    assert blocked.status_code == 422
    again = client.post(f"/api/merge/branches/{branch_id}/seal").json()
    assert again["seal_id"] == receipt["seal_id"]
    assert again["buckets"] == receipt["buckets"]


def test_receipt_rejects_unsealed(client):
    ingest_main(client)
    branch_id = make_branch(client, SH, seal=False)
    assert client.get(f"/api/merge/branches/{branch_id}/receipt").status_code == 422


def test_receipts_read_back_original_per_site(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    receipts = {branch_id: client.get(f"/api/merge/branches/{branch_id}/receipt").json()
                for branch_id in ids}
    outcome = merge(client, ids).json()
    for branch_id, receipt in receipts.items():
        assert outcome["receipts"][branch_id]["digest"] == receipt["digest"]
        assert outcome["receipts"][branch_id]["buckets"] == receipt["buckets"]


def test_buckets_recomputed_not_summed(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    merge(client, ids)
    buckets = {(row["hour"], row["service"]): row
               for row in client.get("/api/merge/main").json()["buckets"]}
    # 两地快照都含 r2，合并后只有一个 r2 事件，不能相加成 2
    assert buckets[("2026-11-15T02:00:00Z", "pay")]["count"] == 1
    # r1 有冲突，所在 01:00/pay 桶保持主库旧值；SH 新增的 r3 同桶也不提前写入
    assert buckets[("2026-11-15T01:00:00Z", "pay")]["count"] == 1
    # 其他桶照常应用
    assert buckets[("2026-11-15T03:00:00Z", "search")]["count"] == 1


def test_conflict_keeps_both_candidates_without_blocking_other_buckets(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    outcome = merge(client, ids).json()
    assert outcome["reason"] == "CONFLICT"
    conflicts = outcome["conflicts"]
    assert len(conflicts) == 1
    assert conflicts[0]["request_id"] == "r1"
    assert conflicts[0]["reason_code"] == "DUPLICATE_CONFLICT"
    candidates = conflicts[0]["candidates"]
    assert any(item["side"] == "main" for item in candidates)
    assert any(item["side"].startswith("br-") for item in candidates)
    assert {item["payload"]["status"] for item in candidates} >= {200, 404, 500}
    # 未裁决桶保持主库旧值
    bucket = client.get(f"/api/merge/{outcome['merge_id']}/buckets").json()
    held = [item for item in bucket["buckets"] if item["state"] == "HELD"]
    assert held and held[0]["old"]["count"] == held[0]["new"]["count"]
    # 其他桶照常应用
    applied = {(item["hour"], item["service"]): item for item in bucket["buckets"]
               if item["state"] == "APPLIED"}
    assert ("2026-11-15T03:00:00Z", "search") in applied


def test_conflict_requires_reason(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    conflict = merge(client, ids).json()["conflicts"][0]
    response = client.post(f"/api/merge/conflicts/{conflict['conflict_id']}/resolve",
                           json={"choice": "KEEP_MAIN"})
    assert response.status_code == 422
    assert client.get("/api/merge/conflicts").json()["count"] == 1


def test_resolve_candidate_applies_held_contribution(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    outcome = merge(client, ids).json()
    conflict = outcome["conflicts"][0]
    chosen = next(item for item in conflict["candidates"] if item["payload"]["status"] == 500)
    response = client.post(f"/api/merge/conflicts/{conflict['conflict_id']}/resolve",
                           json={"content_hash": chosen["content_hash"], "reason": "以 SH 复核为准"})
    assert response.status_code == 200, response.text
    view = response.json()
    assert view["open_conflicts"] == 0
    assert view["held_count"] == 0
    events = {item["request_id"]: item for item in client.get("/api/merge/main").json()["events"]}
    assert events["r1"]["status"] == 500
    assert client.get(f"/api/merge/{outcome['merge_id']}/buckets").json()["held"] == []


def test_resolve_keep_main_keeps_old_value(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    conflict = merge(client, ids).json()["conflicts"][0]
    response = client.post(f"/api/merge/conflicts/{conflict['conflict_id']}/resolve",
                           json={"choice": "KEEP_MAIN", "reason": "主库为准"})
    assert response.status_code == 200
    events = {item["request_id"]: item for item in client.get("/api/merge/main").json()["events"]}
    assert events["r1"]["status"] == 200


def test_stale_version_rejected(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    version = client.get("/api/merge/main").json()["version"]
    stale = merge(client, ids, version - 1)
    assert stale.status_code == 422
    assert stale.json()["error"] == "STALE_VERSION"
    missing = client.post("/api/merge", json={"branch_ids": ids})
    assert missing.status_code == 422
    assert client.get("/api/merge/completed").json()["count"] == 0


def test_merge_requires_sealed_branch(client):
    ingest_main(client)
    branch_id = make_branch(client, SH, seal=False)
    response = merge(client, [branch_id])
    assert response.status_code == 422
    assert response.json()["error"] == "BRANCH_NOT_SEALED"


def test_retry_does_not_double_count(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    first = merge(client, ids).json()
    buckets_before = client.get("/api/merge/main").json()["buckets"]
    second = merge(client, ids).json()
    assert second.get("reused") is True
    assert second["merge_id"] == first["merge_id"]
    assert client.get("/api/merge/main").json()["buckets"] == buckets_before


def test_resume_is_idempotent_after_interruption(client, monkeypatch):
    ingest_main(client)
    branch_id = make_branch(client, {"site": "SH", "logs": [
        '{"request_id":"b1","timestamp":"2026-11-15T02:00:00Z","service":"pay","status":200}',
        '{"request_id":"b2","timestamp":"2026-11-15T04:00:00Z","service":"pay","status":200}',
    ]})
    service = client.app.state.merge_service
    original = service.store.apply_contrib
    calls = {"n": 0}

    def interrupted(merge_id, contribution):
        if calls["n"] >= 1:
            raise RuntimeError("interrupted")
        calls["n"] += 1
        return original(merge_id, contribution)

    monkeypatch.setattr(service.store, "apply_contrib", interrupted)
    version = client.get("/api/merge/main").json()["version"]
    with pytest.raises(RuntimeError):
        service.merge({"branch_ids": [branch_id], "expected_version": version})
    monkeypatch.setattr(service.store, "apply_contrib", original)

    pending = client.get("/api/merge/pending").json()
    assert pending["count"] == 1
    merge_id = pending["pending"][0]["merge_id"]
    resumed = client.post(f"/api/merge/{merge_id}/resume")
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "COMPLETED"
    buckets = {(row["hour"], row["service"]): row["count"]
               for row in client.get("/api/merge/main").json()["buckets"]}
    assert buckets[("2026-11-15T02:00:00Z", "pay")] == 2
    assert buckets[("2026-11-15T04:00:00Z", "pay")] == 1
    client.post(f"/api/merge/{merge_id}/resume")
    buckets_again = client.get("/api/merge/main").json()["buckets"]
    assert buckets_again == client.get("/api/merge/main").json()["buckets"]
    events = [item["request_id"] for item in client.get("/api/merge/main").json()["events"]]
    assert sorted(events) == sorted(set(events))
    assert {(row["hour"], row["service"]): row["count"] for row in buckets_again}[
        ("2026-11-15T02:00:00Z", "pay")] == 2


def test_undo_reverts_only_this_merge(client):
    ingest_main(client)
    before = client.get("/api/merge/main").json()
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    outcome = merge(client, ids).json()
    conflict = outcome["conflicts"][0]
    client.post(f"/api/merge/conflicts/{conflict['conflict_id']}/resolve",
                json={"choice": "KEEP_MAIN", "reason": "主库为准"})
    merged = client.get("/api/merge/main").json()
    assert merged["digest"] != before["digest"]

    undone = client.post(f"/api/merge/{outcome['merge_id']}/undo", json={}).json()
    assert undone["state"] == "UNDONE"
    assert undone["undo"]["reverted"] >= 1
    after = client.get("/api/merge/main").json()
    assert after["digest"] == before["digest"]
    assert after["buckets"] == before["buckets"]
    assert client.get("/api/merge/conflicts").json()["count"] == 1


def test_undo_is_idempotent(client):
    ingest_main(client)
    outcome = merge(client, [make_branch(client, SH)]).json()
    first = client.post(f"/api/merge/{outcome['merge_id']}/undo", json={}).json()
    second = client.post(f"/api/merge/{outcome['merge_id']}/undo", json={}).json()
    assert first["state"] == second["state"] == "UNDONE"
    assert second.get("reused") is True


def test_undo_does_not_clobber_later_main_change(client):
    ingest_main(client)
    outcome = merge(client, [make_branch(client, SH)]).json()
    client.post("/api/merge/main/ingest", json={"logs": [
        '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}']})
    undone = client.post(f"/api/merge/{outcome['merge_id']}/undo", json={})
    assert undone.status_code == 200
    # 合并没有改动 r1，撤销不应删除主库既有事件
    ids = [item["request_id"] for item in client.get("/api/merge/main").json()["events"]]
    assert "r1" in ids and "r2" in ids


def test_readback_endpoints(client):
    ingest_main(client)
    ids = [make_branch(client, SH), make_branch(client, SZ)]
    outcome = merge(client, ids).json()
    assert client.get("/api/merge/pending").json()["count"] == 0
    completed = client.get("/api/merge/completed").json()
    assert completed["count"] == 1
    assert completed["completed"][0]["merge_id"] == outcome["merge_id"]
    detail = client.get(f"/api/merge/{outcome['merge_id']}").json()
    assert detail["conflict_count"] == 1
    assert len(detail["receipts"]) == 2
    conflicts = client.get("/api/merge/conflicts").json()
    assert conflicts["count"] == 1
    state = client.get("/api/merge/state").json()
    assert state["main"]["version"] == 2


def test_timezone_and_dedup_rules_unchanged(client):
    # 同时刻不同偏移归一到同一小时桶；DST fold=1 仍按 M2 规则
    response = client.post("/api/merge/main/ingest", json={"logs": [
        '{"request_id":"z1","timestamp":"2026-11-15T09:05:00+08:00","service":"pay","status":200}',
        '{"request_id":"z2","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}',
    ]})
    assert response.status_code == 200
    buckets = client.get("/api/merge/main").json()["buckets"]
    assert buckets == [{"hour": "2026-11-15T01:00:00Z", "service": "pay",
                        "count": 2, "errors": 0}]
    # 同一请求编号同值只保留一条
    duplicate = client.post("/api/merge/main/ingest", json={"logs": [
        '{"request_id":"z1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}']})
    assert duplicate.json()["duplicates"] == 1
    assert client.get("/api/merge/main").json()["events"].__len__() == 2


def test_core_common_ancestor():
    base = [{"request_id": "a", "timestamp": "2026-11-15T01:00:00Z", "service": "pay",
             "status": 200, "extras": {}}]
    other = [{"request_id": "a", "timestamp": "2026-11-15T01:00:00Z", "service": "pay",
              "status": 200, "extras": {}},
             {"request_id": "b", "timestamp": "2026-11-15T02:00:00Z", "service": "pay",
              "status": 200, "extras": {}}]
    ancestor = core.common_ancestor([base, other])
    assert [item["request_id"] for item in ancestor] == ["a"]
