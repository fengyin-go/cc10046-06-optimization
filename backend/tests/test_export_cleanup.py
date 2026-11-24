"""结果存储流式导出与数据清理冲突处理的测试。

覆盖：
- 流式导出与旧的一次性拼装输出逐字节一致（筛选、表头、顺序、字段内容）；
- 三十万行导出分块传输，不把全量文本留在内存；
- 导出占用登记：断开/关闭后释放，不再占住待清理批次；
- 数据清理按归档批次保留期限回收，报表区分已回收/仍待回收；
- 导出与清理冲突时，被占用批次不回收、不少行、不重复。
"""
import asyncio
import csv
import io
import sys
import os
import time
import tracemalloc
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from app.routers.resource import export_entries
from app.routers.chains import run_cleanup_scan
from app.services import service_for
from app.services.data_cleanup import DataCleanupService
from app.services.export_guard import ExportGuard, export_guard
from app.store import store

MODULE = "result_store"
FIELDS = service_for(MODULE).LIST_FIELDS


@pytest.fixture(autouse=True)
def restore_store():
    saved = {
        name: [dict(row) for row in store.rows(name)]
        for name in (MODULE, "data_cleanup")
    }
    yield
    for name, rows in saved.items():
        store.rows(name)[:] = rows


def make_request(query_string: bytes = b""):
    from starlette.requests import Request
    return Request({
        "type": "http", "method": "GET",
        "path": f"/api/{MODULE}/export", "query_string": query_string,
        "headers": [],
    })


def collect_text(response) -> str:
    async def _run():
        parts = []
        async for chunk in response.body_iterator:
            parts.append(chunk if isinstance(chunk, str) else chunk.decode("utf-8"))
        return "".join(parts)
    return asyncio.run(_run())


def legacy_export_text(service, items) -> str:
    """改动前路由里的一次性拼装逻辑，作为内容基准。"""
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
    return output.getvalue()


def seed_batch(entry_id, batch_id, status, collected_on, **extra):
    row = {
        "id": entry_id,
        "批次标识": batch_id,
        "来源配置": "cfg-live",
        "采集时间": collected_on,
        "原始行数": "100",
        "有效行数": "95",
        "存储路径": f"/data/results/{batch_id}.json.gz",
        "压缩格式": "gzip",
        "校验摘要": f"md5:{batch_id[:8]}",
        "status": status,
        "pending": False,
        "abnormal": False,
    }
    row.update(extra)
    store.rows(MODULE).append(row)
    return row


def test_export_streaming_matches_legacy_bytes():
    service = service_for(MODULE)
    seed_batch(9001, "B-FORMULA", "已归档", "2026-01-01", 来源配置="=SUM(1)")
    seed_batch(9002, "B-MINUS", "已落库", "2026-02-02", 存储路径="-path")

    response = export_entries(MODULE, make_request(), None, None)
    streamed = collect_text(response)
    expected = legacy_export_text(service, service.snapshot_entries())
    assert streamed == expected
    assert streamed.startswith("\ufeffid,批次标识")
    # 公式转义行为保持原样
    assert "'=SUM(1)" in streamed
    assert "'-path" in streamed


def test_export_filters_keyword_status_unchanged():
    service = service_for(MODULE)
    seed_batch(9101, "B-FILTER-A", "已归档", "2026-01-01")
    seed_batch(9102, "B-FILTER-B", "已落库", "2026-01-02")

    response = export_entries(
        MODULE, make_request("status=已归档".encode()), None, "已归档")
    streamed = collect_text(response)
    items = service.snapshot_entries(status="已归档")
    assert streamed == legacy_export_text(service, items)
    assert "B-FILTER-A" in streamed and "B-FILTER-B" not in streamed

    response = export_entries(
        MODULE, make_request("keyword=FILTER-B".encode()), "FILTER-B", None)
    streamed = collect_text(response)
    items = service.snapshot_entries(keyword="FILTER-B")
    assert streamed == legacy_export_text(service, items)
    assert "B-FILTER-B" in streamed and "B-FILTER-A" not in streamed


def test_export_300k_rows_streams_without_full_text_in_memory():
    rows = store.rows(MODULE)
    rows[:] = []
    for i in range(300_000):
        rows.append({
            "id": i + 1,
            "批次标识": f"BATCH-{i:06d}",
            "来源配置": f"配置源-{i % 17}",
            "采集时间": "2026-10-01",
            "原始行数": str(1000 + i % 100),
            "有效行数": str(900 + i % 90),
            "存储路径": f"/data/results/BATCH-{i:06d}.json.gz",
            "压缩格式": "gzip",
            "校验摘要": f"md5:{i:08x}",
            "status": "已归档" if i % 3 else "已落库",
            "pending": False,
            "abnormal": False,
        })

    tracemalloc.start()
    response = export_entries(MODULE, make_request(), None, None)

    async def consume():
        chunks = 0
        lines = 0
        max_chunk = 0
        first = last = ""
        async for chunk in response.body_iterator:
            chunks += 1
            max_chunk = max(max_chunk, len(chunk))
            for line in chunk.splitlines():
                if first == "":
                    first = line
                last = line
                lines += 1
        return chunks, lines, max_chunk, first, last

    chunks, lines, max_chunk, first, last = asyncio.run(consume())
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert lines == 300_001  # 表头 + 三十万行，一行不少
    assert first.startswith("\ufeffid,批次标识")
    assert last.startswith("300000,BATCH-299999")
    assert chunks > 100  # 分块吐出，不是一整块
    assert max_chunk < 512 * 1024  # 单块有界
    # 峰值内存远小于整份文本（全量文本约 45MB）
    assert peak < 25 * 1024 * 1024, f"峰值内存 {peak / 1024 / 1024:.1f}MB 超出流式预算"
    # 导出结束后占用应已释放
    assert export_guard.pinned_ids(MODULE) == set()


def test_export_guard_refcount_and_release():
    guard = ExportGuard()
    token_a = guard.acquire(MODULE, [1, 2, 3])
    token_b = guard.acquire(MODULE, [3, 4])
    assert guard.pinned_ids(MODULE) == {1, 2, 3, 4}
    guard.release(token_a)
    assert guard.pinned_ids(MODULE) == {3, 4}
    guard.release(token_b)
    assert guard.pinned_ids(MODULE) == set()
    # 重复释放不报错
    guard.release(token_b)
    assert guard.pinned_ids(MODULE) == set()


def test_export_abort_releases_hold():
    seed_batch(9201, "B-ABORT", "已归档", "2026-01-01")
    response = export_entries(MODULE, make_request(), None, None)
    assert export_guard.pinned_ids(MODULE) != set()

    async def abort_midway():
        agen = response.body_iterator
        await agen.__anext__()  # 只拿第一块就断开
        await agen.aclose()

    asyncio.run(abort_midway())
    assert export_guard.pinned_ids(MODULE) == set()


def test_export_full_consume_releases_hold():
    seed_batch(9301, "B-DONE", "已归档", "2026-01-01")
    response = export_entries(MODULE, make_request(), None, None)
    collect_text(response)
    assert export_guard.pinned_ids(MODULE) == set()


def test_cleanup_reclaims_by_retention_and_reports():
    today = datetime.now().strftime("%Y-%m-%d")
    old = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    seed_batch(9401, "B-FRESH", "已归档", today)
    seed_batch(9402, "B-OLD", "已归档", old)
    seed_batch(9403, "B-LIVE", "已落库", old)

    svc = DataCleanupService()
    report = svc.domain_cleanup("CLEAN-T1", MODULE, retention_days=30)

    assert report["任务标识"] == "CLEAN-T1"
    assert report["目标模块"] == MODULE
    assert report["已回收"] == ["B-OLD"]
    pending = {item["批次标识"]: item["原因"] for item in report["仍待回收"]}
    assert pending == {"B-FRESH": "保留期内"}
    assert report["预计影响"] == "2"
    assert report["已回收数"] == "1" and report["待回收数"] == "1"

    remaining = {row["批次标识"] for row in store.rows(MODULE)}
    assert "B-OLD" not in remaining
    assert {"B-FRESH", "B-LIVE"} <= remaining

    # 报表条目本身也登记在清理任务列表里
    entry = store.rows("data_cleanup")[-1]
    assert entry["任务标识"] == "CLEAN-T1"
    assert entry["已回收数"] == "1"


def test_cleanup_skips_batches_pinned_by_export():
    old = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    seed_batch(9501, "B-PINNED", "已归档", old)
    svc = DataCleanupService()

    hold = export_guard.acquire(MODULE, [9501])
    try:
        report = svc.domain_cleanup("CLEAN-T2", MODULE, retention_days=30)
        assert report["已回收"] == []
        assert report["仍待回收"] == [{"批次标识": "B-PINNED", "原因": "导出占用中"}]
        assert "B-PINNED" in {row["批次标识"] for row in store.rows(MODULE)}
    finally:
        export_guard.release(hold)

    report = svc.domain_cleanup("CLEAN-T3", MODULE, retention_days=30)
    assert report["已回收"] == ["B-PINNED"]
    assert "B-PINNED" not in {row["批次标识"] for row in store.rows(MODULE)}


def test_export_snapshot_immune_to_concurrent_cleanup():
    """导出快照不受清理增删影响：不少行、不重复。"""
    from app.routers.resource import _csv_chunks
    seed_batch(9601, "B-SNAP-1", "已归档", "2026-01-01")
    seed_batch(9602, "B-SNAP-2", "已归档", "2026-01-02")
    service = service_for(MODULE)
    snapshot = service.snapshot_entries(status="已归档")
    expected = legacy_export_text(service, snapshot)

    # 模拟清理在导出传输期间回收了别的行、又新增了行
    store.rows(MODULE)[:] = [
        row for row in store.rows(MODULE) if row.get("批次标识") != "B-SNAP-1"
    ]
    seed_batch(9603, "B-SNAP-3", "已归档", "2026-01-03")

    # 在途导出仍按快照逐行产出：被回收的行照发、新增的行不混入
    inflight = "".join(_csv_chunks(service, snapshot))
    assert inflight == expected
    assert "B-SNAP-1" in inflight and "B-SNAP-3" not in inflight
    lines = inflight.splitlines()
    assert len(lines) == len(set(lines))  # 无重复行

    # 新发起的导出反映清理后的最新数据
    streamed = collect_text(export_entries(
        MODULE, make_request("status=已归档".encode()), None, "已归档"))
    assert "B-SNAP-1" not in streamed
    assert "B-SNAP-2" in streamed and "B-SNAP-3" in streamed


def test_cleanup_scan_chain_returns_report():
    old = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    seed_batch(9701, "B-CHAIN", "已归档", old)
    result = run_cleanup_scan({
        "task_id": "CLEAN-CHAIN",
        "target_module": MODULE,
        "retention_days": 30,
    })
    assert result["chain"] == "cleanup-scan"
    report = result["report"]
    assert report["任务标识"] == "CLEAN-CHAIN"
    assert report["已回收"] == ["B-CHAIN"]
    assert report["仍待回收"] == []
