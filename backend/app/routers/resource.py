"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any, Iterator

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from starlette.concurrency import iterate_in_threadpool

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.export_guard import export_guard

router = APIRouter(prefix="/api", tags=["业务资源"])

# 单次吐出的 CSV 文本达到该阈值就冲刷给客户端，避免整份文件占住内存。
EXPORT_CHUNK_BYTES = 64 * 1024


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


def _csv_chunks(service, items: list[dict[str, Any]]) -> Iterator[str]:
    """逐行生成 CSV 文本块。

    表头、字段顺序、公式转义与记录顺序和原一次性拼装完全一致，
    但每攒够一个块就交出，三十万行也不会把全量文本留在内存。
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    buffer.write("\ufeff")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
        if buffer.tell() >= EXPORT_CHUNK_BYTES:
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)
    tail = buffer.getvalue()
    if tail:
        yield tail


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
    # 快照只持有行引用：首块立即开始下载，且导出期间清理任务不会
    # 让本次传输中途少行或重复。
    items = service.snapshot_entries(filters=filters, keyword=keyword, status=status)
    hold = export_guard.acquire(service.MODULE, (row.get("id", 0) for row in items))

    async def stream():
        try:
            async for chunk in iterate_in_threadpool(_csv_chunks(service, items)):
                yield chunk
        finally:
            # 正常发完、客户端断开或生成器被关闭都会走到这里，
            # 已结束的请求不会继续占住待清理的批次。
            export_guard.release(hold)

    return StreamingResponse(
        stream(), media_type="text/csv; charset=utf-8",
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
