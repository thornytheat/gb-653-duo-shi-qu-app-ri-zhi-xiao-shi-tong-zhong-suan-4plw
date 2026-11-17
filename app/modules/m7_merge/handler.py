from fastapi import APIRouter, Body, Request

from app.modules.m7_merge.service import MergeService
from . import core

router = APIRouter(tags=["M7-离线分支合并"])

SAMPLE = {
    "main": {"logs": [
        '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}',
        '{"request_id":"r2","timestamp":"2026-11-15T02:05:00Z","service":"pay","status":200}',
        '{"request_id":"r3","timestamp":"2026-11-15T05:05:00Z","service":"search","status":200}',
    ]},
    "branches": [
        {"site": "SH", "logs": [
            '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":500}',
            '{"request_id":"r4","timestamp":"2026-11-15T01:30:00Z","service":"pay","status":200}',
        ]},
        {"site": "SZ", "logs": [
            '{"request_id":"r1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":404}',
            '{"request_id":"r6","timestamp":"2026-11-15T03:10:00Z","service":"search","status":200}',
        ]},
    ],
}


def service(request: Request) -> MergeService:
    return request.app.state.merge_service


def _page(request: Request, data: dict):
    return request.app.state.templates.TemplateResponse(
        request, "m7_merge.html", {"title": "离线分支合并", "data": data})


# ---------- 页面 ----------
@router.get("/merge")
def page(request: Request):
    return _page(request, service(request).state())


@router.get("/merge/new")
def page_new(request: Request):
    return _page(request, service(request).state())


@router.get("/merge/pending")
def page_pending(request: Request):
    return _page(request, service(request).state())


@router.get("/merge/completed")
def page_completed(request: Request):
    return _page(request, service(request).state())


# ---------- 回读 ----------
@router.get("/api/merge/state")
def api_state(request: Request):
    return service(request).state()


@router.get("/api/merge/pending")
def api_pending(request: Request):
    return service(request).pending()


@router.get("/api/merge/completed")
def api_completed(request: Request):
    return service(request).completed()


@router.get("/api/merge/conflicts")
def api_conflicts(request: Request, merge_id: str | None = None, status: str | None = None):
    return service(request).conflicts(merge_id=merge_id, status=status)


@router.get("/api/merge/main")
def api_main(request: Request):
    return service(request).main_state()


@router.get("/api/merge/{merge_id}")
def api_merge(request: Request, merge_id: str):
    return service(request).merge_detail(merge_id)


@router.get("/api/merge/{merge_id}/buckets")
def api_bucket_diff(request: Request, merge_id: str):
    return service(request).bucket_diff(merge_id)


# ---------- 主库与分支 ----------
@router.post("/api/merge/main/ingest")
def api_ingest_main(request: Request, payload: dict = Body(default={})):
    return service(request).ingest_main(payload or SAMPLE["main"])


@router.post("/api/merge/branches")
def api_create_branch(request: Request, payload: dict = Body(default={})):
    return service(request).create_branch(payload)


@router.get("/api/merge/branches")
def api_branches(request: Request):
    return service(request).branches()


@router.get("/api/merge/branches/{branch_id}")
def api_branch(request: Request, branch_id: str):
    return service(request).branch_detail(branch_id)


@router.post("/api/merge/branches/{branch_id}/logs")
def api_branch_logs(request: Request, branch_id: str, payload: dict = Body(default={})):
    return service(request).append_logs(branch_id, payload)


@router.post("/api/merge/branches/{branch_id}/seal")
def api_branch_seal(request: Request, branch_id: str, payload: dict = Body(default={})):
    return service(request).seal_branch(branch_id)


@router.get("/api/merge/branches/{branch_id}/receipt")
def api_branch_receipt(request: Request, branch_id: str):
    return service(request).branch_receipt(branch_id)


# ---------- 合并控制 ----------
@router.post("/api/merge")
def api_merge_run(request: Request, payload: dict = Body(default={})):
    return service(request).merge(payload)


@router.post("/api/merge/demo")
def api_merge_demo(request: Request):
    """按样例生成主库、两个站点分支并合并，便于一次复核。"""
    svc = service(request)
    svc.ingest_main(SAMPLE["main"])
    base = svc.main_state()
    branch_ids = []
    for item in SAMPLE["branches"]:
        created = svc.create_branch({"site": item["site"], "logs": item["logs"]})
        branch_ids.append(created["branch_id"])
        svc.seal_branch(created["branch_id"])
    outcome = svc.merge({"branch_ids": branch_ids, "expected_version": base["version"]})
    return outcome


@router.post("/api/merge/{merge_id}/resume")
def api_merge_resume(request: Request, merge_id: str):
    return service(request).resume(merge_id)


@router.post("/api/merge/{merge_id}/undo")
def api_merge_undo(request: Request, merge_id: str, payload: dict = Body(default={})):
    return service(request).undo(merge_id, payload or {})


@router.post("/api/merge/conflicts/{conflict_id}/resolve")
def api_resolve(request: Request, conflict_id: str, payload: dict = Body(default={})):
    return service(request).resolve(conflict_id, payload or {})
