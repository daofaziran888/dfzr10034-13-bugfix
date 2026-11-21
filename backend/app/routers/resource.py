"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ── 清理备份回读 / 回滚 ───────────────────────────────────────
# 固定路径必须在 /{module}/{entry_id} 之外单独注册，避免被数字 id 规则抢匹配。

@router.post("/retention_job/{entry_id}/dry-run", response_model=dict)
def dry_run_cleanup(entry_id: int, data_rows: int, retention_days: str | None = None) -> dict[str, Any]:
    """预演清理：首次预演写入清理前恢复快照，再次预演给出两组候选行数。"""
    service = require_service("retention_job")
    result = service.dry_run_cleanup(entry_id, data_rows, retention_days=retention_days)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "预演失败"))
    return result


@router.post("/retention_job/{entry_id}/policy-correction", response_model=dict)
def correct_retention_policy(
    entry_id: int,
    retention_days: str | None = None,
    next_run: str | None = None,
    candidate_count: int | None = None,
) -> dict[str, Any]:
    """清理期间到达的策略订正：只改当前计划，不改冻结备份。"""
    service = require_service("retention_job")
    result = service.correct_policy(
        entry_id, retention_days=retention_days, next_run=next_run,
        candidate_count=candidate_count,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "订正失败"))
    return result


@router.get("/retention_job/{entry_id}/backup", response_model=dict)
def get_cleanup_backup(entry_id: int) -> dict[str, Any]:
    service = require_service("retention_job")
    backup = service.get_cleanup_backup(entry_id)
    if backup is None:
        raise HTTPException(status_code=404, detail=f"清理计划 {entry_id} 没有清理前恢复快照")
    return backup


@router.post("/retention_job/{entry_id}/restore", response_model=dict)
def restore_cleanup(entry_id: int, snapshot_id: str | None = None) -> dict[str, Any]:
    service = require_service("retention_job")
    result = service.restore_cleanup(entry_id, snapshot_id=snapshot_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "回滚失败"))
    return result


@router.get("/version_history/{entry_id}/snapshot", response_model=dict)
def get_snapshot_detail(entry_id: int, snapshot_ref: str | None = None) -> dict[str, Any]:
    service = require_service("version_history")
    reference = snapshot_ref or str(entry_id)
    detail = service.get_snapshot_detail(reference)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"版本快照 {reference} 不存在")
    return detail
