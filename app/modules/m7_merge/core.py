"""M7 离线分支合并的纯规则层。

复用既有模块，不改变时区、请求去重与小时桶规则：
- M1 日志行采信（source_line / TRUNCATED）
- M2 时间归一（UTC / fold / DST 间隔拒绝）
- M3 请求去重（同一 request_id 先到先得，异值记冲突）
- M4 小时分桶（UTC 小时、左闭右开）

合并以“实际日志”为事实来源：分支保存派生时的实际日志快照与离线新增日志，
合并比较共同祖先、当前主库与各分支的事件，桶值只由合并后的实际事件重算，
既不做桶计数覆盖，也不把两地桶值相加。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from app.manifest import canonical_digest
from app.modules.m1_logs import core as m1
from app.modules.m2_time import core as m2
from app.modules.m3_events import core as m3
from app.modules.m4_buckets import core as m4

CONTENT_FIELDS = ("request_id", "timestamp", "service", "status", "extras")
_ROW_META_FIELDS = {
    "request_id", "timestamp", "service", "status", "extras",
    "source_line", "zone", "fold", "origin",
}


def dump(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def content_of(event: dict) -> dict:
    return {field: event.get(field) for field in CONTENT_FIELDS}


def content_json(event: dict) -> str:
    return dump(content_of(event))


def content_hash(event: dict) -> str:
    return canonical_digest(content_of(event))


def digest_events(events: list[dict]) -> str:
    return canonical_digest([content_of(event) for event in sorted(events, key=lambda e: str(e["request_id"]))])


def common_ancestor(event_lists: list[list[dict]]) -> list[dict]:
    """各分支派生快照的公共事件集合（同编号且同内容才算共同祖先）。"""
    if not event_lists:
        return []
    maps = [{event["request_id"]: content_hash(event) for event in events}
            for events in event_lists]
    common = set(maps[0])
    for item in maps[1:]:
        common &= set(item)
    template = {event["request_id"]: event for event in event_lists[0]}
    merged: dict[str, dict] = {}
    for request_id in common:
        hashes = {item[request_id] for item in maps}
        if len(hashes) == 1:
            merged[request_id] = template[request_id]
    return [merged[key] for key in sorted(merged)]


def event_key(event: dict) -> tuple[str, str]:
    """桶键：UTC 小时（右端开）+ 服务。"""
    return (m4.hour_key(event["timestamp"]), str(event["service"]))


def bucket_ref(event: dict) -> dict:
    hour, service = event_key(event)
    return {"hour": hour, "service": service}


def bucket_rows(events: list[dict]) -> list[dict]:
    return m4.bucket([
        {"timestamp": event["timestamp"], "service": event["service"], "status": event["status"]}
        for event in events
    ])


def bucket_index(rows: list[dict]) -> dict[tuple[str, str], dict]:
    return {(row["hour"], row["service"]): row for row in rows}


def bucket_key(ref: dict) -> tuple[str, str]:
    return (ref["hour"], ref["service"])


def make_event(row: dict, source_line: int, default_zone: str | None = None,
               default_fold: int = 0, origin: str = "log") -> dict:
    """把一条实际日志行按 M2 规则归一并收成事件；失败抛 ValueError。"""
    request_id = str(row.get("request_id", ""))
    if request_id == "":
        raise ValueError("EMPTY_VALUE")
    timestamp = row.get("timestamp")
    if timestamp in (None, ""):
        raise ValueError("EMPTY_VALUE")
    service = str(row.get("service", ""))
    if service == "":
        raise ValueError("EMPTY_VALUE")
    zone = row.get("zone", default_zone)
    fold = int(row.get("fold", default_fold) or 0)
    normalized = m2.normalize(timestamp, zone, fold)
    try:
        status = int(row.get("status"))
    except (TypeError, ValueError):
        raise ValueError("INVALID_VALUE")
    extras = {key: row[key] for key in sorted(row) if key not in _ROW_META_FIELDS}
    return {
        "request_id": request_id,
        "timestamp": normalized,
        "service": service,
        "status": status,
        "extras": extras,
        "source_line": source_line,
        "zone": zone,
        "fold": fold,
        "origin": origin,
    }


def read_log_rows(payload: dict) -> tuple[list[dict], list[dict]]:
    """接受 text（原始日志文本）或 logs（行字符串/对象列表）。"""
    logs = payload.get("logs")
    if logs is not None and not isinstance(logs, str):
        if all(isinstance(item, dict) for item in logs):
            rows = []
            for number, item in enumerate(logs, 1):
                row = dict(item)
                row["request_id"] = str(row.get("request_id", ""))
                row["source_line"] = number
                rows.append(row)
            return rows, []
        logs = "\n".join(str(item) for item in logs)
    text = logs if isinstance(logs, str) else payload.get("text")
    if text is None:
        return [], []
    parsed = m1.parse(text)
    return parsed["rows"], parsed["quarantine"]


def events_from_payload(payload: dict, default_zone: str | None = None,
                        origin: str = "log") -> dict:
    rows, malformed = read_log_rows(payload)
    events: list[dict] = []
    quarantine: list[dict] = list(malformed)
    for row in rows:
        try:
            events.append(make_event(row, int(row.get("source_line", len(events) + 1)),
                                     payload.get("zone", default_zone),
                                     payload.get("fold", 0), origin))
        except ValueError as exc:
            quarantine.append({
                "source_line": row.get("source_line"),
                "reason": str(exc) or "INVALID_VALUE",
                "raw": dump(row),
            })
    return {"events": events, "quarantine": quarantine}


def side_map(events: list[dict]) -> tuple[dict[str, dict], list[dict]]:
    """按 M3 规则收敛一侧事件：同键先到先得，同值记重复，异值记冲突。"""
    items = [content_of(event) for event in events]
    result = m3.dedupe(items)
    lookup: dict[tuple[str, str], dict] = {}
    for event in events:
        lookup.setdefault((event["request_id"], content_hash(event)), event)
    accepted: dict[str, dict] = {}
    for item in result["accepted"]:
        digest = canonical_digest({field: item.get(field) for field in CONTENT_FIELDS})
        accepted[item["request_id"]] = lookup.get((item["request_id"], digest), dict(item))
    return accepted, result["conflict"]


def _candidate(side: str, event: dict, site: str | None = None) -> dict:
    item = {
        "side": side,
        "content_hash": content_hash(event),
        "payload": content_of(event),
    }
    if site is not None:
        item["site"] = site
    return item


def _unique_candidates(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for item in items:
        if item["content_hash"] in seen:
            continue
        seen.add(item["content_hash"])
        out.append(item)
    return out


def plan_merge(ancestor_events: list[dict], main_events: list[dict],
               branches: list[dict], merge_id: str) -> dict:
    """三方比较，返回决策、冲突与需保持主库旧值的桶。

    branches: [{"branch_id": str, "site": str, "events": [..]}]
    """
    ancestor, _ = side_map(ancestor_events)
    main, _ = side_map(main_events)
    branch_maps = [
        {"branch_id": item["branch_id"], "site": item.get("site"),
         "map": side_map(item.get("events", []))[0]}
        for item in branches
    ]
    keys = set(ancestor) | set(main)
    for item in branch_maps:
        keys |= set(item["map"])

    decisions: list[dict] = []
    conflicts: list[dict] = []
    for request_id in sorted(keys):
        base = ancestor.get(request_id)
        current = main.get(request_id)
        sides = [
            {"branch_id": item["branch_id"], "site": item["site"],
             "event": item["map"].get(request_id)}
            for item in branch_maps
        ]
        present = [item for item in sides if item["event"] is not None]
        if current is None and not present:
            continue

        values: list[dict] = []
        if current is not None:
            values.append(_candidate("main", current))
        for item in present:
            values.append(_candidate(item["branch_id"], item["event"], item.get("site")))
        distinct = _unique_candidates(values)

        if len(distinct) == 1:
            if current is not None:
                decisions.append({"request_id": request_id, "action": "KEEP",
                                  "event": current, "content_hash": content_hash(current),
                                  "previous": None})
            else:
                chosen = present[0]["event"]
                decisions.append({"request_id": request_id, "action": "INSERT",
                                  "event": chosen, "content_hash": content_hash(chosen),
                                  "previous": None})
            continue

        base_hash = content_hash(base) if base is not None else None
        if base_hash is None:
            conflicts.append(_conflict(merge_id, request_id, None, distinct))
            continue

        main_changed = current is not None and content_hash(current) != base_hash
        changed = [item for item in present if content_hash(item["event"]) != base_hash]

        if not main_changed:
            if not changed:
                decisions.append({"request_id": request_id, "action": "KEEP",
                                  "event": current, "content_hash": content_hash(current),
                                  "previous": None})
                continue
            hashes = {content_hash(item["event"]) for item in changed}
            if len(hashes) == 1:
                chosen = changed[0]["event"]
                decisions.append({"request_id": request_id, "action": "REPLACE",
                                  "event": chosen, "content_hash": content_hash(chosen),
                                  "previous": current})
                continue
            conflicts.append(_conflict(merge_id, request_id, base,
                                       _unique_candidates(
                                           ([_candidate("main", current)] if current is not None else []) +
                                           [_candidate(item["branch_id"], item["event"], item.get("site"))
                                            for item in changed])))
            continue

        if not changed:
            decisions.append({"request_id": request_id, "action": "KEEP",
                              "event": current, "content_hash": content_hash(current),
                              "previous": None})
            continue
        if all(content_hash(item["event"]) == content_hash(current) for item in changed):
            decisions.append({"request_id": request_id, "action": "KEEP",
                              "event": current, "content_hash": content_hash(current),
                              "previous": None})
            continue
        candidates = [_candidate("main", current)] + [
            _candidate(item["branch_id"], item["event"], item.get("site")) for item in changed
        ]
        conflicts.append(_conflict(merge_id, request_id, base, _unique_candidates(candidates)))

    held_refs: dict[tuple[str, str], dict] = {}
    for conflict in conflicts:
        for ref in conflict["buckets"]:
            held_refs[bucket_key(ref)] = ref

    contributions: list[dict] = []
    for decision in decisions:
        if decision["action"] == "KEEP":
            continue
        ref = bucket_ref(decision["event"])
        contributions.append({
            "request_id": decision["request_id"],
            "action": decision["action"],
            "content_hash": decision["content_hash"],
            "payload": content_of(decision["event"]),
            "full_payload": decision["event"],
            "previous": decision.get("previous"),
            "bucket": ref,
            "held": 1 if bucket_key(ref) in held_refs else 0,
        })

    return {
        "decisions": decisions,
        "contributions": contributions,
        "conflicts": conflicts,
        "held_buckets": [held_refs[key] for key in sorted(held_refs)],
    }


def _conflict(merge_id: str, request_id: str, ancestor: dict | None,
              candidates: list[dict]) -> dict:
    conflict_id = "cf-" + canonical_digest([merge_id, request_id])[:16]
    refs: dict[tuple[str, str], dict] = {}
    for candidate in candidates:
        ref = bucket_ref(candidate["payload"])
        refs[bucket_key(ref)] = ref
    buckets = [refs[key] for key in sorted(refs)]
    primary = bucket_ref(candidates[0]["payload"])
    return {
        "conflict_id": conflict_id,
        "request_id": request_id,
        "ancestor": content_of(ancestor) if ancestor is not None else None,
        "candidates": candidates,
        "buckets": buckets,
        "primary": primary,
        "reason_code": "DUPLICATE_CONFLICT",
    }


def compose_branch_events(base_events: list[dict], appended: list[dict]) -> dict:
    """合成分支的有效实际事件。

    起点快照只是分支派生时的主库材料，不参与分支内部的请求去重；
    分支自有日志之间仍按 M3 规则去重（同键先到先得、异值记冲突），
    自有日志可修订起点快照中的同编号事件。
    """
    accepted, side_conflicts = side_map(appended)
    effective = {event["request_id"]: event for event in base_events}
    for request_id, event in accepted.items():
        effective[request_id] = event
    return {
        "events": [effective[key] for key in sorted(effective)],
        "conflicts": side_conflicts,
    }


def seal_receipt(branch: dict, events: list[dict],
                 side_conflicts: list[dict] | None = None) -> dict:
    """对分支自身日志封账；回执一经生成即为不可变原结果。"""
    accepted, conflicts = side_map(events)
    if side_conflicts:
        conflicts = list(side_conflicts) + list(conflicts)
    rows = bucket_rows(list(accepted.values()))
    receipt = {
        "seal_id": "seal-" + canonical_digest({"branch_id": branch["branch_id"],
                                                "digest": branch.get("base_digest"),
                                                "events": [content_of(e) for e in accepted.values()]})[:16],
        "branch_id": branch["branch_id"],
        "site": branch.get("site"),
        "base_version": branch.get("base_version"),
        "base_digest": branch.get("base_digest"),
        "event_count": len(events),
        "accepted_count": len(accepted),
        "quarantine_count": len(side_conflicts),
        "duplicate_count": max(0, len(events) - len(accepted) - len(side_conflicts)),
        "conflicts": side_conflicts,
        "buckets": rows,
        "bucket_count": len(rows),
        "sealed_at": now_utc(),
    }
    receipt["digest"] = canonical_digest({key: receipt[key] for key in receipt if key != "digest"})
    return receipt


def bucket_diffs(before: list[dict], after: list[dict],
                 held_refs: list[dict]) -> list[dict]:
    """桶差异：只回读发生变化的桶与仍被冲突扣住的桶。"""
    old = bucket_index(before)
    new = bucket_index(after)
    held = {bucket_key(ref) for ref in held_refs}
    rows: list[dict] = []
    for key in sorted(set(old) | set(new) | held):
        left = old.get(key)
        right = new.get(key)
        if key in held:
            rows.append({
                "hour": key[0], "service": key[1],
                "state": "HELD",
                "old": left,
                "new": left,
                "candidate": right,
                "delta_count": 0,
                "delta_errors": 0,
            })
            continue
        if left == right:
            continue
        rows.append({
            "hour": key[0], "service": key[1],
            "state": "APPLIED",
            "old": left,
            "new": right,
            "delta_count": (right or {}).get("count", 0) - (left or {}).get("count", 0),
            "delta_errors": (right or {}).get("errors", 0) - (left or {}).get("errors", 0),
        })
    return rows
