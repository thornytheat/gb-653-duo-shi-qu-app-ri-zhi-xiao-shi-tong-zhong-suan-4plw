"""M7 离线分支合并的服务编排。

流程：
1. 主库从实际日志导入（`ingest_main`），版本只在内容变化时前进。
2. 各站点从主库当时的实际日志快照生成分支（`create_branch`），可继续追加离线日志。
3. 分支封账（`seal_branch`）生成不可变回执，之后回读只返回该回执原结果。
4. 合并（`merge`）：用共同祖先 + 主库当前 + 各分支实际事件做三方比较；
   桶只按合并后的实际事件重算，不做桶值覆盖或相加。
5. 未裁决冲突所在桶保持主库旧值，其他桶照常应用。
6. 合并任务逐条写入并记录动作，可中断续跑、重试幂等、撤销只回冲本次增量。
"""
from __future__ import annotations

from app.errors import DomainError
from app.modules.m7_merge import core
from app.modules.m7_merge.store import MergeStore

ACTIVE_STATUSES = {"APPLYING", "COMPLETED"}


class MergeService:
    def __init__(self, store: MergeStore):
        self.store = store

    # ---------- 主库 ----------
    def main_state(self) -> dict:
        state = self.store.main_state()
        state["branch_count"] = len(self.store.branches())
        state["conflict_count"] = len(self.store.conflicts(status="OPEN"))
        return state

    def ingest_main(self, payload: dict) -> dict:
        """把实际日志并入主库：同一请求编号先到先得，异值拒绝并留因。"""
        parsed = core.events_from_payload(payload, origin="main")
        current = self.store.main_events_map()
        accepted: list[dict] = []
        duplicates: list[dict] = []
        rejected: list[dict] = []
        for event in parsed["events"]:
            request_id = event["request_id"]
            existing = current.get(request_id)
            if existing is None:
                accepted.append(event)
                continue
            if core.content_hash(existing) == core.content_hash(event):
                duplicates.append({"request_id": request_id,
                                   "reason": "DUPLICATE_CONFLICT",
                                   "content_hash": core.content_hash(event)})
                continue
            rejected.append({
                "request_id": request_id,
                "reason_code": "DUPLICATE_CONFLICT",
                "candidates": [
                    {"side": "main", "content_hash": core.content_hash(existing),
                     "payload": core.content_of(existing)},
                    {"side": "log", "content_hash": core.content_hash(event),
                     "payload": core.content_of(event)},
                ],
            })
        if rejected:
            raise DomainError("DUPLICATE_CONFLICT",
                              f"{len(rejected)} 条日志与主库既有事件取值冲突，未写入")
        result = self.store.append_main(accepted)
        return {
            "status": "READY",
            "input_count": len(parsed["events"]) + len(parsed["quarantine"]),
            "valid_count": len(accepted) + len(duplicates),
            "quarantine_count": len(parsed["quarantine"]),
            "accepted": len(accepted),
            "duplicates": len(duplicates),
            "rejected": [],
            "version": result["version"],
            "changed": result["changed"],
            "digest": result["digest"],
            "reason": "OK",
        }

    # ---------- 分支 ----------
    def create_branch(self, payload: dict) -> dict:
        site = str(payload.get("site") or "").strip()
        if not site:
            raise DomainError("INVALID_VALUE", "缺少站点标识 site")
        base = self.store.main_state()
        created = self.store.create_branch(site, base)
        branch_id = created["branch_id"]
        appended = 0
        if payload.get("logs") is not None or payload.get("text") is not None:
            appended = self.append_logs(branch_id, payload)["appended"]
        receipt = None
        if payload.get("seal"):
            receipt = self.seal_branch(branch_id)
        return {
            "status": "READY",
            "branch_id": branch_id,
            "site": site,
            "reused": created["reused"],
            "base_version": created["base_version"],
            "base_digest": base["digest"],
            "base_event_count": len(base["events"]),
            "appended": appended,
            "event_count": len(self.store.branch_events(branch_id)),
            "sealed": receipt is not None,
            "receipt": receipt,
            "reason": "OK",
        }

    def append_logs(self, branch_id: str, payload: dict) -> dict:
        branch = self._branch_or_404(branch_id)
        if branch["status"] == "SEALED":
            raise DomainError("STALE_VERSION", "分支已封账，不能追加日志")
        parsed = core.events_from_payload(payload, origin=f"branch:{branch_id}")
        self.store.append_branch_events(branch_id, parsed["events"], parsed["quarantine"])
        return {
            "status": "READY",
            "branch_id": branch_id,
            "appended": len(parsed["events"]),
            "quarantine_count": len(parsed["quarantine"]),
            "event_count": len(self.store.branch_events(branch_id)),
            "sealed": False,
            "reason": "OK" if not parsed["quarantine"] else "TRUNCATED",
        }

    def seal_branch(self, branch_id: str) -> dict:
        branch = self._branch_or_404(branch_id)
        existing = self.store.branch_receipt(branch_id)
        if existing is not None:
            return existing
        events = self.store.branch_events(branch_id)
        _, side_conflicts = core.side_map(self.store.branch_own_events(branch_id))
        receipt = core.seal_receipt(branch, events, side_conflicts)
        self.store.seal_branch(branch_id, receipt)
        return receipt

    def branch_detail(self, branch_id: str) -> dict:
        branch = self._branch_or_404(branch_id)
        events = self.store.branch_events(branch_id)
        return {
            "status": "READY",
            "branch_id": branch_id,
            "site": branch["site"],
            "state": branch["status"],
            "base_version": branch["base_version"],
            "base_digest": branch["base_digest"],
            "base_event_count": len(self.store.branch_base_events(branch_id)),
            "event_count": len(events),
            "own_event_count": len(self.store.branch_own_events(branch_id)),
            "events": [core.content_of(event) for event in events],
            "quarantine": self.store.branch_quarantine(branch_id),
            "buckets": core.bucket_rows(events),
            "sealed_at": branch["sealed_at"],
            "merge_id": branch["merge_id"],
            "reason": "OK",
        }

    def branch_receipt(self, branch_id: str) -> dict:
        receipt = self.store.branch_receipt(branch_id)
        if receipt is None:
            raise DomainError("INVALID_VALUE", "分支尚未封账，无回执可回读")
        return receipt

    def branches(self) -> dict:
        items = self.store.branches()
        return {"branches": [
            {
                "branch_id": item["branch_id"],
                "site": item["site"],
                "state": item["status"],
                "base_version": item["base_version"],
                "base_digest": item["base_digest"],
                "event_count": item["event_count"],
                "sealed_at": item["sealed_at"],
                "seal_id": (item["seal"] or {}).get("seal_id"),
                "merge_id": item["merge_id"],
            } for item in items
        ]}

    # ---------- 合并 ----------
    def merge(self, payload: dict) -> dict:
        branch_ids = [str(value) for value in payload.get("branch_ids") or []]
        if not branch_ids:
            raise DomainError("INVALID_VALUE", "缺少 branch_ids")
        if len(set(branch_ids)) != len(branch_ids):
            raise DomainError("INVALID_VALUE", "branch_ids 不得重复")
        branches = [self._branch_or_404(branch_id) for branch_id in branch_ids]
        for branch in branches:
            if branch["status"] != "SEALED":
                raise DomainError("BRANCH_NOT_SEALED",
                                  f"分支 {branch['branch_id']} 尚未封账")

        signature = self.store.signature(branch_ids)
        previous = self.store.find_merge_by_signature(signature)
        if previous is not None and previous["status"] == "APPLYING":
            if payload.get("dry_run"):
                return self._merge_view(previous["merge_id"]) | {"dry_run": True}
            self.apply_pending(previous["merge_id"])
            return self._merge_view(previous["merge_id"]) | {"reused": True}
        if previous is not None and previous["status"] == "COMPLETED":
            if payload.get("dry_run"):
                return self._merge_view(previous["merge_id"]) | {"dry_run": True}
            return self._merge_view(previous["merge_id"]) | {"reused": True}

        expected = payload.get("expected_version")
        if expected is None:
            raise DomainError("STALE_VERSION", "缺少 expected_version，拒绝无版本合并")
        try:
            expected = int(expected)
        except (TypeError, ValueError):
            raise DomainError("STALE_VERSION", "expected_version 必须为整数")
        current = self.store.version()
        if expected != current:
            raise DomainError("STALE_VERSION",
                              f"主库版本已前进到 {current}，拒绝基于 {expected} 的合并")

        ancestor = core.common_ancestor(
            [self.store.branch_base_events(branch_id) for branch_id in branch_ids])
        attempt = self.store.count_merges(signature) + 1
        merge_id = "mg-" + core.canonical_digest([signature, expected, attempt])[:16]
        plan = core.plan_merge(ancestor, self.store.main_events(), [
            {"branch_id": branch["branch_id"], "site": branch["site"],
             "events": self.store.branch_events(branch["branch_id"])}
            for branch in branches
        ], merge_id)

        if payload.get("dry_run"):
            return {
                "status": "READY",
                "merge_id": merge_id,
                "dry_run": True,
                "base_version": expected,
                "target_version": expected + 1,
                "ancestor_count": len(ancestor),
                "plan": _plan_summary(plan),
                "conflicts": plan["conflicts"],
                "held_buckets": plan["held_buckets"],
                "reason": "CONFLICT" if plan["conflicts"] else "OK",
            }

        record = {
            "merge_id": merge_id,
            "signature": signature,
            "status": "APPLYING",
            "base_version": expected,
            "target_version": expected + 1,
            "branch_ids": branch_ids,
            "ancestor_digest": core.digest_events(ancestor),
            "plan": _plan_summary(plan),
            "before_buckets": self.store.main_buckets(),
            "created_at": core.now_utc(),
            "updated_at": core.now_utc(),
        }
        try:
            self.store.begin_merge(record, expected, plan)
        except ValueError as exc:
            raise DomainError(str(exc), f"主库版本已前进到 {self.store.version()}，拒绝旧版合并")
        for branch in branches:
            self.store.attach_branch(branch["branch_id"], merge_id)
        self.apply_pending(merge_id)
        return self._merge_view(merge_id) | {"reused": False}

    def resume(self, merge_id: str) -> dict:
        merge = self._merge_or_404(merge_id)
        if merge["status"] == "UNDONE":
            raise DomainError("STALE_VERSION", "合并已撤销，不能续跑")
        self.apply_pending(merge_id)
        return self._merge_view(merge_id) | {"resumed": True}

    def apply_pending(self, merge_id: str) -> dict:
        """幂等应用尚未应用的贡献；中断后重复调用不会重复计数。

        每次都对本次合并所有已应用贡献所在桶重算，避免崩溃在
        “写入事件”与“重算桶”之间时该桶永久滞后。
        """
        applied = 0
        pending = self.store.contribs(merge_id, applied=0)
        for contribution in pending:
            if contribution["held"]:
                continue
            if self.store.apply_contrib(merge_id, contribution):
                applied += 1
        buckets = {core.bucket_key(item["bucket"]) for item in self.store.contribs(merge_id)
                   if item["applied"] and not item["held"]}
        if buckets:
            self.store.recompute_buckets(buckets)
        if self.store.merge(merge_id)["status"] == "APPLYING":
            self.store.set_merge_status(merge_id, "COMPLETED", completed_at=core.now_utc())
        return {"applied": applied, "buckets": len(buckets)}

    def resolve(self, conflict_id: str, payload: dict) -> dict:
        conflict = self.store.conflict(conflict_id)
        if conflict is None:
            raise DomainError("INVALID_VALUE", "冲突不存在")
        if conflict["status"] != "OPEN":
            raise DomainError("STALE_VERSION", "冲突已裁决，不能重复裁决")
        merge = self.store.merge(conflict["merge_id"])
        if merge is None or merge["status"] == "UNDONE":
            raise DomainError("STALE_VERSION", "所属合并已撤销，不能裁决")
        reason = str(payload.get("reason") or "").strip()
        if not reason:
            raise DomainError("INVALID_VALUE", "裁决必须留下原因 reason")
        reason_code = str(payload.get("reason_code") or "MANUAL_DECISION")

        choice = str(payload.get("choice") or "")
        candidate_hash = payload.get("content_hash")
        before = self.store.main_events_map().get(conflict["request_id"])
        chosen: dict | None = None
        if choice in ("KEEP_MAIN", "MAIN"):
            if before is None:
                raise DomainError("INVALID_VALUE", "主库没有该请求的旧值，无法保留主库")
            chosen = {"side": "main", "content_hash": core.content_hash(before),
                      "payload": core.content_of(before)}
        elif choice.upper() == "DROP":
            chosen = None
        else:
            if choice:
                candidate_hash = candidate_hash or choice
            chosen = next((item for item in conflict["candidates"]
                           if item["content_hash"] == candidate_hash), None)
            if chosen is None:
                raise DomainError("INVALID_VALUE", "content_hash 未匹配到候选")
        resolution = {
            "choice": "DROP" if chosen is None else (
                "KEEP_MAIN" if chosen["side"] == "main" else "TAKE_CANDIDATE"),
            "content_hash": chosen["content_hash"] if chosen else None,
            "side": chosen["side"] if chosen else None,
            "event": chosen["payload"] if chosen else None,
            "reason": reason,
            "reason_code": reason_code,
        }
        if chosen is not None:
            event = self._materialize(conflict["request_id"], chosen["payload"],
                                      before, conflict["merge_id"])
            self.store.log_action(conflict["merge_id"], conflict["request_id"],
                                  before, event, "RESOLVE")
            self.store.write_main_event(event, conflict["merge_id"])
        else:
            if before is not None:
                self.store.log_action(conflict["merge_id"], conflict["request_id"],
                                      before, None, "RESOLVE")
                self.store.delete_main_event(conflict["request_id"])
        self.store.resolve_conflict(conflict_id, resolution, reason)
        self._release_held(merge["merge_id"], conflict)
        self._refresh_status(merge["merge_id"])
        return self._merge_view(merge["merge_id"])

    def undo(self, merge_id: str, payload: dict | None = None) -> dict:
        merge = self._merge_or_404(merge_id)
        if merge["status"] == "UNDONE":
            return self._merge_view(merge_id) | {"reused": True}
        expected = (payload or {}).get("expected_version")
        if expected is not None and int(expected) != self.store.version():
            raise DomainError("STALE_VERSION",
                              f"主库版本已前进到 {self.store.version()}，拒绝基于 {expected} 的撤销")
        reverted, skipped = self.store.revert_actions(merge_id)
        buckets: set[tuple[str, str]] = set()
        for item in reverted:
            for ref in item["refs"]:
                buckets.add(core.bucket_key(ref))
        self.store.recompute_buckets(buckets)
        for conflict in self.store.conflicts(merge_id=merge_id, status="RESOLVED"):
            self.store.reopen_conflict(conflict["conflict_id"])
        self.store.reset_contribs(merge_id)
        self.store.set_merge_status(merge_id, "UNDONE", undone_at=core.now_utc())
        self.store.bump_version()
        view = self._merge_view(merge_id)
        view["undo"] = {
            "reverted": len(reverted),
            "skipped": skipped,
            "buckets": sorted(hour + "|" + service for hour, service in buckets),
        }
        return view

    # ---------- 回读 ----------
    def pending(self) -> dict:
        merges = [item for item in self.store.merges() if item["status"] == "APPLYING"]
        return {"pending": [self._merge_brief(item) for item in merges],
                "count": len(merges)}

    def completed(self) -> dict:
        merges = [item for item in self.store.merges() if item["status"] == "COMPLETED"]
        return {"completed": [self._merge_brief(item) for item in merges],
                "count": len(merges)}

    def conflicts(self, merge_id: str | None = None, status: str | None = None) -> dict:
        items = self.store.conflicts(merge_id=merge_id, status=status)
        return {"conflicts": items, "count": len(items)}

    def merge_detail(self, merge_id: str) -> dict:
        self._merge_or_404(merge_id)
        return self._merge_view(merge_id)

    def bucket_diff(self, merge_id: str) -> dict:
        view = self._merge_view(merge_id)
        return {
            "status": "READY",
            "merge_id": merge_id,
            "state": view["state"],
            "buckets": view["bucket_diff"],
            "held": [item for item in view["bucket_diff"] if item["state"] == "HELD"],
            "applied": [item for item in view["bucket_diff"] if item["state"] == "APPLIED"],
            "reason": "HELD" if view["open_conflicts"] else "OK",
        }

    def state(self) -> dict:
        return {
            "status": "READY",
            "version": self.store.version(),
            "main": self.main_state(),
            "branches": self.branches()["branches"],
            "pending": self.pending()["pending"],
            "completed": self.completed()["completed"],
            "conflicts": self.conflicts(status="OPEN")["conflicts"],
            "reason": "OK",
        }

    # ---------- 内部 ----------
    def _merge_view(self, merge_id: str) -> dict:
        merge = self._merge_or_404(merge_id)
        contributions = self.store.contribs(merge_id)
        open_conflicts = self.store.conflicts(merge_id=merge_id, status="OPEN")
        held_refs = [{"hour": item["primary"]["hour"], "service": item["primary"]["service"]}
                     for item in open_conflicts]
        for conflict in open_conflicts:
            held_refs.extend(conflict["buckets"])
        unique_held: dict[tuple[str, str], dict] = {}
        for ref in held_refs:
            unique_held[core.bucket_key(ref)] = ref
        after = self.store.main_buckets()
        if merge["status"] == "UNDONE":
            diff = core.bucket_diffs(merge["before_buckets"], after, [])
        else:
            diff = core.bucket_diffs(merge["before_buckets"], after,
                                     [unique_held[key] for key in sorted(unique_held)])
        receipts = {}
        for branch_id in merge["branch_ids"]:
            receipt = self.store.branch_receipt(branch_id)
            if receipt is not None:
                receipts[branch_id] = {
                    "seal_id": receipt["seal_id"],
                    "site": receipt["site"],
                    "accepted_count": receipt["accepted_count"],
                    "bucket_count": receipt["bucket_count"],
                    "digest": receipt["digest"],
                    "buckets": receipt["buckets"],
                }
        return {
            "status": "READY",
            "merge_id": merge_id,
            "state": merge["status"],
            "base_version": merge["base_version"],
            "target_version": merge["target_version"],
            "branch_ids": merge["branch_ids"],
            "ancestor_digest": merge["ancestor_digest"],
            "plan": merge["plan"],
            "contribution_count": len(contributions),
            "applied_count": sum(1 for item in contributions if item["applied"]),
            "held_count": sum(1 for item in contributions if item["held"] and not item["applied"]),
            "conflict_count": len(self.store.conflicts(merge_id=merge_id)),
            "open_conflicts": len(open_conflicts),
            "conflicts": self.store.conflicts(merge_id=merge_id),
            "receipts": receipts,
            "bucket_diff": diff,
            "created_at": merge["created_at"],
            "completed_at": merge["completed_at"],
            "undone_at": merge["undone_at"],
            "reason": "CONFLICT" if open_conflicts else "OK",
        }

    def _merge_brief(self, merge: dict) -> dict:
        open_conflicts = len(self.store.conflicts(merge_id=merge["merge_id"], status="OPEN"))
        return {
            "merge_id": merge["merge_id"],
            "state": merge["status"],
            "base_version": merge["base_version"],
            "target_version": merge["target_version"],
            "branch_ids": merge["branch_ids"],
            "open_conflicts": open_conflicts,
            "created_at": merge["created_at"],
            "completed_at": merge["completed_at"],
            "undone_at": merge["undone_at"],
        }

    def _release_held(self, merge_id: str, conflict: dict) -> None:
        remaining = self.store.conflicts(merge_id=merge_id, status="OPEN")
        still_held: set[tuple[str, str]] = set()
        for item in remaining:
            for ref in item["buckets"]:
                still_held.add(core.bucket_key(ref))
        candidate_keys = {core.bucket_key(ref) for ref in conflict["buckets"]}
        released = candidate_keys - still_held
        if not released:
            return
        buckets: set[tuple[str, str]] = set()
        for contribution in self.store.release_held_contribs(merge_id, released):
            if self.store.apply_contrib(merge_id, contribution):
                buckets.add(core.bucket_key(contribution["bucket"]))
        self.store.clear_held(merge_id, released)
        self.store.recompute_buckets(buckets | released)

    def _refresh_status(self, merge_id: str) -> None:
        merge = self.store.merge(merge_id)
        if merge["status"] == "APPLYING":
            self.store.set_merge_status(merge_id, "COMPLETED", completed_at=core.now_utc())

    def _materialize(self, request_id: str, payload: dict, previous: dict | None,
                     merge_id: str) -> dict:
        if previous is not None:
            event = dict(previous)
        else:
            event = {"request_id": request_id, "source_line": 0, "zone": None,
                     "fold": 0, "origin": f"merge:{merge_id}"}
        event["request_id"] = request_id
        for field in ("timestamp", "service", "status", "extras"):
            event[field] = payload.get(field)
        event["origin"] = f"merge:{merge_id}"
        return event

    def _branch_or_404(self, branch_id: str) -> dict:
        branch = self.store.branch(branch_id)
        if branch is None:
            raise DomainError("INVALID_VALUE", f"分支 {branch_id} 不存在")
        return branch

    def _merge_or_404(self, merge_id: str) -> dict:
        merge = self.store.merge(merge_id)
        if merge is None:
            raise DomainError("INVALID_VALUE", f"合并 {merge_id} 不存在")
        return merge


def _plan_summary(plan: dict) -> dict:
    return {
        "decisions": [
            {"request_id": item["request_id"], "action": item["action"],
             "content_hash": item["content_hash"]}
            for item in plan["decisions"] if item["action"] != "KEEP"
        ],
        "contribution_count": len(plan["contributions"]),
        "conflict_count": len(plan["conflicts"]),
        "conflict_ids": [item["conflict_id"] for item in plan["conflicts"]],
        "held_buckets": plan["held_buckets"],
    }
