"""M7 离线分支合并的持久化层。

与既有 `runs` 表共用同一个 SQLite 文件，但使用独立命名空间，不影响既有模块。
合并计划在单个事务里落盘（任务 + 贡献 + 冲突），之后的应用逐条幂等推进；
每次主库变更都记入动作日志，撤销时按日志逆序只回冲本次增量。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.modules.m7_merge import core

SCHEMA = """
CREATE TABLE IF NOT EXISTS m7_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_main (
    request_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    event TEXT NOT NULL,
    updated_merge TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m7_buckets (
    hour TEXT NOT NULL,
    service TEXT NOT NULL,
    count INTEGER NOT NULL,
    errors INTEGER NOT NULL,
    PRIMARY KEY (hour, service)
);
CREATE TABLE IF NOT EXISTS m7_branches (
    branch_id TEXT PRIMARY KEY,
    site TEXT NOT NULL,
    status TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    base_digest TEXT NOT NULL,
    base_events TEXT NOT NULL,
    base_buckets TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sealed_at TEXT,
    seal_receipt TEXT,
    merge_id TEXT
);
CREATE TABLE IF NOT EXISTS m7_branch_events (
    branch_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event TEXT NOT NULL,
    PRIMARY KEY (branch_id, seq)
);
CREATE TABLE IF NOT EXISTS m7_branch_quarantine (
    branch_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    item TEXT NOT NULL,
    PRIMARY KEY (branch_id, seq)
);
CREATE TABLE IF NOT EXISTS m7_merges (
    merge_id TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    status TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    branch_ids TEXT NOT NULL,
    ancestor_digest TEXT NOT NULL,
    plan TEXT NOT NULL,
    before_buckets TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    undone_at TEXT
);
CREATE TABLE IF NOT EXISTS m7_contrib (
    merge_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    event TEXT NOT NULL,
    previous TEXT,
    hour TEXT NOT NULL,
    service TEXT NOT NULL,
    held INTEGER NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0,
    applied_at TEXT,
    conflict_id TEXT,
    PRIMARY KEY (merge_id, seq),
    UNIQUE (merge_id, request_id, content_hash)
);
CREATE TABLE IF NOT EXISTS m7_conflicts (
    conflict_id TEXT PRIMARY KEY,
    merge_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    ancestor TEXT,
    candidates TEXT NOT NULL,
    buckets TEXT NOT NULL,
    primary_hour TEXT NOT NULL,
    primary_service TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    status TEXT NOT NULL,
    resolution TEXT,
    resolution_reason TEXT,
    resolved_at TEXT
);
CREATE TABLE IF NOT EXISTS m7_actions (
    merge_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    before_hash TEXT,
    before_event TEXT,
    after_hash TEXT,
    after_event TEXT,
    reverted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (merge_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_m7_conflicts_merge ON m7_conflicts (merge_id, status);
CREATE INDEX IF NOT EXISTS idx_m7_contrib_merge ON m7_contrib (merge_id, applied);
CREATE INDEX IF NOT EXISTS idx_m7_actions_merge ON m7_actions (merge_id, reverted);
"""


class MergeStore:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
            self._meta(db, "version", "0")

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        return db

    @staticmethod
    def _meta(db: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
        row = db.execute("SELECT value FROM m7_meta WHERE key=?", (key,)).fetchone()
        if row is not None:
            return row["value"]
        if default is not None:
            db.execute("INSERT INTO m7_meta VALUES (?, ?)", (key, default))
        return default

    # ---------- 主库 ----------
    def version(self) -> int:
        with self.connect() as db:
            return int(self._meta(db, "version", "0") or 0)

    def main_events(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT event FROM m7_main ORDER BY request_id").fetchall()
        return [json.loads(row["event"]) for row in rows]

    def main_events_map(self) -> dict[str, dict]:
        return {event["request_id"]: event for event in self.main_events()}

    def main_buckets(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT hour, service, count, errors FROM m7_buckets "
                              "ORDER BY hour, service").fetchall()
        return [dict(row) for row in rows]

    def main_state(self) -> dict:
        events = self.main_events()
        return {"version": self.version(), "digest": core.digest_events(events),
                "events": events, "buckets": self.main_buckets()}

    def replace_main(self, events: list[dict]) -> dict:
        digest = core.digest_events(events)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = core.digest_events(_rows_to_events(
                db.execute("SELECT event FROM m7_main ORDER BY request_id").fetchall()))
            if previous == digest:
                return {"version": int(self._meta(db, "version", "0") or 0),
                        "changed": False, "digest": digest}
            db.execute("DELETE FROM m7_main")
            stamp = core.now_utc()
            for event in events:
                db.execute("INSERT INTO m7_main VALUES (?, ?, ?, NULL, ?)",
                           (event["request_id"], core.content_hash(event),
                            core.dump(event), stamp))
            version = int(self._meta(db, "version", "0") or 0) + 1
            db.execute("UPDATE m7_meta SET value=? WHERE key='version'", (str(version),))
            db.execute("DELETE FROM m7_buckets")
            for row in core.bucket_rows(events):
                db.execute("INSERT INTO m7_buckets VALUES (?, ?, ?, ?)",
                           (row["hour"], row["service"], row["count"], row["errors"]))
        return {"version": version, "changed": True, "digest": digest}

    def append_main(self, events: list[dict]) -> dict:
        merged = self.main_events_map()
        for event in events:
            merged[event["request_id"]] = event
        return self.replace_main([merged[key] for key in sorted(merged)])

    # ---------- 分支 ----------
    def create_branch(self, site: str, base: dict) -> dict:
        branch_id = "br-" + core.canonical_digest(
            {"site": site, "base_digest": base["digest"],
             "version": base["version"]})[:12]
        with self.connect() as db:
            existing = db.execute("SELECT * FROM m7_branches WHERE branch_id=?",
                                  (branch_id,)).fetchone()
            if existing is not None:
                return {"branch_id": branch_id, "reused": True,
                        "base_version": existing["base_version"]}
            db.execute(
                "INSERT INTO m7_branches VALUES (?, ?, 'OPEN', ?, ?, ?, ?, ?, NULL, NULL, NULL)",
                (branch_id, site, base["version"], base["digest"],
                 core.dump(base["events"]), core.dump(base["buckets"]), core.now_utc()))
        return {"branch_id": branch_id, "reused": False, "base_version": base["version"]}

    def branch(self, branch_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM m7_branches WHERE branch_id=?",
                             (branch_id,)).fetchone()
        return dict(row) if row is not None else None

    def branches(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT * FROM m7_branches ORDER BY created_at, branch_id").fetchall()
            counts = {row["branch_id"]: row["n"] for row in db.execute(
                "SELECT branch_id, COUNT(*) AS n FROM m7_branch_events GROUP BY branch_id")}
        out = []
        for row in rows:
            item = dict(row)
            item["event_count"] = counts.get(item["branch_id"], 0)
            item["seal"] = json.loads(row["seal_receipt"]) if row["seal_receipt"] else None
            out.append(item)
        return out

    def branch_own_events(self, branch_id: str) -> list[dict]:
        """分支自有离线日志形成的事件（不含起点快照）。"""
        with self.connect() as db:
            rows = db.execute("SELECT event FROM m7_branch_events WHERE branch_id=? ORDER BY seq",
                              (branch_id,)).fetchall()
        return [json.loads(row["event"]) for row in rows]

    def branch_events(self, branch_id: str) -> list[dict]:
        """分支有效事件：起点快照 + 自有日志，自有日志可修订同编号事件。"""
        branch = self.branch(branch_id)
        if branch is None:
            return []
        return core.compose_branch_events(
            json.loads(branch["base_events"]), self.branch_own_events(branch_id))["events"]

    def branch_base_events(self, branch_id: str) -> list[dict]:
        branch = self.branch(branch_id)
        return json.loads(branch["base_events"]) if branch is not None else []

    def branch_quarantine(self, branch_id: str) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT item FROM m7_branch_quarantine WHERE branch_id=? ORDER BY seq",
                              (branch_id,)).fetchall()
        return [json.loads(row["item"]) for row in rows]

    def append_branch_events(self, branch_id: str, events: list[dict],
                             quarantine: list[dict]) -> int:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            seq = db.execute("SELECT COALESCE(MAX(seq), 0) FROM m7_branch_events WHERE branch_id=?",
                             (branch_id,)).fetchone()[0]
            for event in events:
                seq += 1
                db.execute("INSERT INTO m7_branch_events VALUES (?, ?, ?)",
                           (branch_id, seq, core.dump(event)))
            qseq = db.execute("SELECT COALESCE(MAX(seq), 0) FROM m7_branch_quarantine "
                              "WHERE branch_id=?", (branch_id,)).fetchone()[0]
            for item in quarantine:
                qseq += 1
                db.execute("INSERT INTO m7_branch_quarantine VALUES (?, ?, ?)",
                           (branch_id, qseq, core.dump(item)))
        return seq

    def seal_branch(self, branch_id: str, receipt: dict) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT seal_receipt FROM m7_branches WHERE branch_id=?",
                             (branch_id,)).fetchone()
            if row is not None and row["seal_receipt"]:
                return json.loads(row["seal_receipt"])
            db.execute("UPDATE m7_branches SET status='SEALED', sealed_at=?, seal_receipt=? "
                       "WHERE branch_id=?",
                       (receipt["sealed_at"], core.dump(receipt), branch_id))
        return receipt

    def branch_receipt(self, branch_id: str) -> dict | None:
        branch = self.branch(branch_id)
        if branch is None or not branch["seal_receipt"]:
            return None
        return json.loads(branch["seal_receipt"])

    def signature(self, branch_ids: list[str]) -> str:
        seals = []
        for branch_id in sorted(branch_ids):
            receipt = self.branch_receipt(branch_id)
            seals.append({"branch_id": branch_id,
                          "seal": receipt["seal_id"] if receipt else None,
                          "base_version": (self.branch(branch_id) or {}).get("base_version")})
        return core.canonical_digest(seals)

    def find_merge_by_signature(self, signature: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT merge_id FROM m7_merges WHERE signature=? "
                             "ORDER BY created_at DESC, rowid DESC LIMIT 1",
                             (signature,)).fetchone()
        return self.merge(row["merge_id"]) if row is not None else None

    def count_merges(self, signature: str) -> int:
        with self.connect() as db:
            return db.execute("SELECT COUNT(*) FROM m7_merges WHERE signature=?",
                              (signature,)).fetchone()[0]

    def attach_branch(self, branch_id: str, merge_id: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE m7_branches SET merge_id=? WHERE branch_id=?",
                       (merge_id, branch_id))

    # ---------- 合并任务 ----------
    def begin_merge(self, record: dict, expected: int, plan: dict) -> None:
        """原子落盘：版本 CAS、任务、贡献与冲突。失败抛 STALE_VERSION。"""
        held_bucket_keys = {core.bucket_key(ref) for ref in plan["held_buckets"]}
        conflict_by_request = {item["request_id"]: item["conflict_id"]
                               for item in plan["conflicts"]}
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = int(self._meta(db, "version", "0") or 0)
            if current != expected:
                raise ValueError("STALE_VERSION")
            db.execute(
                "INSERT INTO m7_merges VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
                (record["merge_id"], record["signature"], record["status"],
                 record["base_version"], record["target_version"],
                 core.dump(record["branch_ids"]), record["ancestor_digest"],
                 core.dump(record["plan"]), core.dump(record["before_buckets"]),
                 record["created_at"], record["updated_at"]))
            db.execute("UPDATE m7_meta SET value=? WHERE key='version'",
                       (str(expected + 1),))
            for seq, item in enumerate(plan["contributions"], 1):
                key = core.bucket_key(item["bucket"])
                held = 1 if key in held_bucket_keys else 0
                db.execute(
                    "INSERT OR IGNORE INTO m7_contrib VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)",
                    (record["merge_id"], seq, item["request_id"], item["action"],
                     item["content_hash"], core.dump(item["full_payload"]),
                     core.dump(item["previous"]) if item.get("previous") is not None else None,
                     item["bucket"]["hour"], item["bucket"]["service"], held,
                     conflict_by_request.get(item["request_id"]) if held else None))
            for conflict in plan["conflicts"]:
                db.execute(
                    "INSERT OR IGNORE INTO m7_conflicts VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', NULL, NULL, NULL)",
                    (conflict["conflict_id"], record["merge_id"], conflict["request_id"],
                     core.dump(conflict["ancestor"]) if conflict["ancestor"] is not None else None,
                     core.dump(conflict["candidates"]), core.dump(conflict["buckets"]),
                     conflict["primary"]["hour"], conflict["primary"]["service"],
                     conflict["reason_code"]))

    def merge(self, merge_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM m7_merges WHERE merge_id=?", (merge_id,)).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["branch_ids"] = json.loads(item["branch_ids"])
        item["plan"] = json.loads(item["plan"])
        item["before_buckets"] = json.loads(item["before_buckets"])
        return item

    def merges(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("SELECT merge_id FROM m7_merges ORDER BY created_at, rowid").fetchall()
        return [self.merge(row["merge_id"]) for row in rows]

    def set_merge_status(self, merge_id: str, status: str, **fields) -> None:
        assignments = ["status=?", "updated_at=?"]
        values: list = [status, core.now_utc()]
        for key, value in fields.items():
            assignments.append(f"{key}=?")
            values.append(value)
        values.append(merge_id)
        with self.connect() as db:
            db.execute(f"UPDATE m7_merges SET {', '.join(assignments)} WHERE merge_id=?", values)

    def contribs(self, merge_id: str, applied: int | None = None) -> list[dict]:
        query = "SELECT * FROM m7_contrib WHERE merge_id=?"
        params: list = [merge_id]
        if applied is not None:
            query += " AND applied=?"
            params.append(applied)
        query += " ORDER BY seq"
        with self.connect() as db:
            rows = db.execute(query, params).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["event"] = json.loads(item["event"])
            item["previous"] = json.loads(item["previous"]) if item["previous"] else None
            item["bucket"] = {"hour": item["hour"], "service": item["service"]}
            out.append(item)
        return out

    def reset_contribs(self, merge_id: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE m7_contrib SET applied=0, applied_at=NULL, held=1 "
                       "WHERE merge_id=? AND held=0", (merge_id,))

    # ---------- 冲突 ----------
    def conflict(self, conflict_id: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM m7_conflicts WHERE conflict_id=?",
                             (conflict_id,)).fetchone()
        return _decode_conflict(row) if row is not None else None

    def conflicts(self, merge_id: str | None = None, status: str | None = None) -> list[dict]:
        query = "SELECT * FROM m7_conflicts WHERE 1=1"
        params: list = []
        if merge_id:
            query += " AND merge_id=?"
            params.append(merge_id)
        if status:
            query += " AND status=?"
            params.append(status)
        query += " ORDER BY conflict_id"
        with self.connect() as db:
            rows = db.execute(query, params).fetchall()
        return [_decode_conflict(row) for row in rows]

    def resolve_conflict(self, conflict_id: str, resolution: dict, reason: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE m7_conflicts SET status='RESOLVED', resolution=?, "
                       "resolution_reason=?, resolved_at=? WHERE conflict_id=?",
                       (core.dump(resolution), reason, core.now_utc(), conflict_id))

    def reopen_conflict(self, conflict_id: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE m7_conflicts SET status='OPEN', resolution=NULL, "
                       "resolution_reason=NULL, resolved_at=NULL WHERE conflict_id=?",
                       (conflict_id,))

    # ---------- 主库写入与动作日志 ----------
    def write_main_event(self, event: dict, merge_id: str) -> None:
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO m7_main VALUES (?, ?, ?, ?, ?)",
                       (event["request_id"], core.content_hash(event), core.dump(event),
                        merge_id, core.now_utc()))

    def delete_main_event(self, request_id: str) -> None:
        with self.connect() as db:
            db.execute("DELETE FROM m7_main WHERE request_id=?", (request_id,))

    def log_action(self, merge_id: str, request_id: str, before: dict | None,
                   after: dict | None, kind: str) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            seq = db.execute("SELECT COALESCE(MAX(seq), 0) FROM m7_actions WHERE merge_id=?",
                             (merge_id,)).fetchone()[0] + 1
            db.execute("INSERT INTO m7_actions VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                       (merge_id, seq, request_id, kind,
                        core.content_hash(before) if before is not None else None,
                        core.dump(before) if before is not None else None,
                        core.content_hash(after) if after is not None else None,
                        core.dump(after) if after is not None else None,
                        core.now_utc()))

    def apply_contrib(self, merge_id: str, contribution: dict) -> bool:
        """幂等应用一条贡献，并记动作日志。已应用返回 False。"""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT applied FROM m7_contrib WHERE merge_id=? AND seq=?",
                             (merge_id, contribution["seq"])).fetchone()
            if row is None or row["applied"]:
                return False
            event = contribution["event"]
            current = db.execute("SELECT event FROM m7_main WHERE request_id=?",
                                 (contribution["request_id"],)).fetchone()
            before = json.loads(current["event"]) if current is not None else None
            if before is not None and core.content_hash(before) == core.content_hash(event):
                db.execute("UPDATE m7_contrib SET applied=1, applied_at=? "
                           "WHERE merge_id=? AND seq=?",
                           (core.now_utc(), merge_id, contribution["seq"]))
                return False
            db.execute("INSERT OR REPLACE INTO m7_main VALUES (?, ?, ?, ?, ?)",
                       (event["request_id"], core.content_hash(event), core.dump(event),
                        merge_id, core.now_utc()))
            seq = db.execute("SELECT COALESCE(MAX(seq), 0) FROM m7_actions WHERE merge_id=?",
                             (merge_id,)).fetchone()[0] + 1
            db.execute("INSERT INTO m7_actions VALUES (?, ?, ?, 'CONTRIB', ?, ?, ?, ?, 0, ?)",
                       (merge_id, seq, contribution["request_id"],
                        core.content_hash(before) if before is not None else None,
                        core.dump(before) if before is not None else None,
                        contribution["content_hash"], core.dump(event), core.now_utc()))
            db.execute("UPDATE m7_contrib SET applied=1, applied_at=? "
                       "WHERE merge_id=? AND seq=?",
                       (core.now_utc(), merge_id, contribution["seq"]))
        return True

    def revert_actions(self, merge_id: str) -> tuple[list[dict], int]:
        """逆序回冲本次合并的动作；仅当主库现值仍等于该动作结果才回冲。"""
        reverted: list[dict] = []
        skipped = 0
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT * FROM m7_actions WHERE merge_id=? AND reverted=0 "
                              "ORDER BY seq DESC", (merge_id,)).fetchall()
            for row in rows:
                current = db.execute("SELECT content_hash FROM m7_main WHERE request_id=?",
                                     (row["request_id"],)).fetchone()
                current_hash = current["content_hash"] if current is not None else None
                if current_hash != row["after_hash"]:
                    skipped += 1
                    continue
                before = json.loads(row["before_event"]) if row["before_event"] else None
                if before is None:
                    db.execute("DELETE FROM m7_main WHERE request_id=?", (row["request_id"],))
                else:
                    db.execute("INSERT OR REPLACE INTO m7_main VALUES (?, ?, ?, NULL, ?)",
                               (row["request_id"], core.content_hash(before),
                                core.dump(before), core.now_utc()))
                db.execute("UPDATE m7_actions SET reverted=1 WHERE merge_id=? AND seq=?",
                           (merge_id, row["seq"]))
                refs = []
                for event in (before, json.loads(row["after_event"]) if row["after_event"] else None):
                    if event is not None:
                        refs.append(core.bucket_ref(event))
                reverted.append({"request_id": row["request_id"], "refs": refs})
        return reverted, skipped

    def recompute_buckets(self, bucket_keys: set[tuple[str, str]]) -> None:
        rows = {core.bucket_key(row): row for row in core.bucket_rows(self.main_events())}
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for hour, service in sorted(bucket_keys):
                row = rows.get((hour, service))
                if row is None:
                    db.execute("DELETE FROM m7_buckets WHERE hour=? AND service=?",
                               (hour, service))
                else:
                    db.execute("INSERT OR REPLACE INTO m7_buckets VALUES (?, ?, ?, ?)",
                               (row["hour"], row["service"], row["count"], row["errors"]))

    def bump_version(self) -> int:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            version = int(self._meta(db, "version", "0") or 0) + 1
            db.execute("UPDATE m7_meta SET value=? WHERE key='version'", (str(version),))
        return version

    def release_held_contribs(self, merge_id: str, bucket_keys: set[tuple[str, str]]) -> list[dict]:
        return [item for item in self.contribs(merge_id, applied=0)
                if int(item["held"]) == 1
                and core.bucket_key(item["bucket"]) in bucket_keys]

    def clear_held(self, merge_id: str, bucket_keys: set[tuple[str, str]]) -> None:
        with self.connect() as db:
            for hour, service in bucket_keys:
                db.execute("UPDATE m7_contrib SET held=0 "
                           "WHERE merge_id=? AND hour=? AND service=?",
                           (merge_id, hour, service))


def _decode_conflict(row: sqlite3.Row) -> dict:
    item = dict(row)
    item["ancestor"] = json.loads(item["ancestor"]) if item["ancestor"] else None
    item["candidates"] = json.loads(item["candidates"])
    item["buckets"] = json.loads(item["buckets"])
    item["resolution"] = json.loads(item["resolution"]) if item["resolution"] else None
    item["primary"] = {"hour": item.pop("primary_hour"),
                       "service": item.pop("primary_service")}
    return item


def _rows_to_events(rows: list[sqlite3.Row]) -> list[dict]:
    return [json.loads(row["event"]) for row in rows]
