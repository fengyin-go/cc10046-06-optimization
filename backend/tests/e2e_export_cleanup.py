"""端到端验证：慢速下载 × 数据清理冲突 × 断开重试。

场景（对应值班员日常）：
1. 造数：一批刚归档（保留期内）、一批超过保留期限、一批已落库，
   再加数千行过期归档批次让下载足够慢；
2. 起真实 uvicorn 服务，慢速下载批次目录 CSV；
3. 下载进行到一半时发起清理扫描 → 传输中的批次全部「仍待回收」；
4. 断开下载 → 已结束的请求释放占用 → 重试清理 → 过期批次「已回收」；
5. 校验新文件内容、已收到数据逐行对上、清理报表区分已回收/仍待回收。

用法：.venv/bin/python tests/e2e_export_cleanup.py
"""
import csv
import http.client
import io
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import uvicorn

from app.services import service_for
from app.services.export_guard import export_guard
from app.store import store

HOST, PORT = "127.0.0.1", 8123
MODULE = "result_store"
FIELDS = service_for(MODULE).LIST_FIELDS
FILLER_ROWS = 200_000       # 过期归档填充行：文件约 22MB，超过回路 TCP 缓冲，
                            # 保证清理发起时服务端仍在传输中段
READ_CHUNK = 65536          # 每次读取字节数
READ_INTERVAL = 0.003       # 慢速下载节奏
CLEANUP_AT_BYTES = 1_500_000  # 收到这么多就发起清理扫描
RETENTION_DAYS = 30

TODAY = datetime.now().strftime("%Y-%m-%d")
OLD_DATE = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

failures = []


def check(name, condition, detail=""):
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {name}{(' — ' + detail) if detail else ''}")
    if not condition:
        failures.append(name)


def make_row(entry_id, batch_id, status, collected_on):
    return {
        "id": entry_id,
        "批次标识": batch_id,
        "来源配置": f"配置源-{entry_id % 13}",
        "采集时间": collected_on,
        "原始行数": str(1000 + entry_id % 100),
        "有效行数": str(900 + entry_id % 90),
        "存储路径": f"/data/results/{batch_id}.json.gz",
        "压缩格式": "gzip",
        "校验摘要": f"md5:{batch_id[:8]}",
        "status": status,
        "pending": False,
        "abnormal": False,
    }


def expected_csv_lines(rows):
    """按导出格式在本地生成期望行，用于逐行核对。"""
    buf = io.StringIO()
    buf.write("\ufeff")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["id", *FIELDS, "status"])
    for row in rows:
        values = [row.get("id"), *(row.get(f, "") for f in FIELDS), row.get("status", "")]
        writer.writerow([
            "'" + str(v) if str(v or "").startswith(("=", "+", "-", "@")) else v
            for v in values
        ])
    return buf.getvalue().splitlines(keepends=True)


def seed():
    rows = store.rows(MODULE)
    rows[:] = []
    rows.append(make_row(1, "B-FRESH-KEEP", "已归档", TODAY))      # 刚归档：保留期内
    rows.append(make_row(2, "B-OLD-EXPIRED", "已归档", OLD_DATE))  # 超过保留期限
    rows.append(make_row(3, "B-LIVE", "已落库", OLD_DATE))         # 未归档：不在清理范围
    for i in range(FILLER_ROWS):
        rows.append(make_row(100 + i, f"B-OLD-FILL-{i:05d}", "已归档", OLD_DATE))
    store.rows("data_cleanup")[:] = []
    return list(rows)


def start_server():
    config = uvicorn.Config("app.main:app", host=HOST, port=PORT, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("服务启动超时")
        time.sleep(0.05)
    return server


def cleanup_scan(task_id):
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    body = json.dumps({
        "task_id": task_id,
        "target_module": MODULE,
        "retention_days": RETENTION_DAYS,
    })
    conn.request("POST", "/api/chains/cleanup-scan", body=body,
                 headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    payload = json.loads(resp.read().decode("utf-8"))
    conn.close()
    return payload["report"]


def download_all():
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    conn.request("GET", f"/api/{MODULE}/export")
    resp = conn.getresponse()
    data = b""
    while True:
        chunk = resp.read(READ_CHUNK)
        if not chunk:
            break
        data += chunk
    conn.close()
    return data


def main():
    print("== 1. 造数 ==")
    seeded = seed()
    print(f"  结果存储共 {len(seeded)} 行：1 刚归档 / {1 + FILLER_ROWS} 过期归档 / 1 已落库")

    print("== 2. 启动服务 ==")
    server = start_server()
    print(f"  服务已监听 {HOST}:{PORT}")

    try:
        print("== 3. 慢速下载批次目录 CSV ==")
        conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
        conn.request("GET", f"/api/{MODULE}/export")
        resp = conn.getresponse()
        started = time.time()
        received = b""
        first_byte_at = None
        report_mid = None
        aborted = False
        while True:
            chunk = resp.read(READ_CHUNK)
            if not chunk:
                break
            if first_byte_at is None:
                first_byte_at = time.time() - started
            received += chunk
            time.sleep(READ_INTERVAL)
            # 下载进行中：发起清理扫描（只发起一次）
            if report_mid is None and len(received) > CLEANUP_AT_BYTES:
                print("== 4. 下载中途发起清理扫描 ==")
                report_mid = cleanup_scan("CLEAN-DURING-DOWNLOAD")
            # 清理结果出来后再读一小段，然后断开下载
            if report_mid is not None and len(received) > CLEANUP_AT_BYTES + 1_000_000:
                aborted = True
                break
        if report_mid is None:
            print("  !! 下载结束后才清理，调大 FILLER_ROWS")
            report_mid = cleanup_scan("CLEAN-DURING-DOWNLOAD")

        print(f"  首字节耗时 {first_byte_at:.3f}s（流式：无需等整份生成）")
        check("清理与下载冲突时，过期批次不回收",
              report_mid["已回收"] == [],
              f"已回收={report_mid['已回收'][:3]}… 共{len(report_mid['已回收'])}条")
        pending = {item["批次标识"]: item["原因"] for item in report_mid["仍待回收"]}
        check("刚归档批次仍待回收（保留期内）",
              pending.get("B-FRESH-KEEP") == "保留期内")
        check("过期批次仍待回收（导出占用中）",
              pending.get("B-OLD-EXPIRED") == "导出占用中")
        check("填充过期批次全部仍待回收（导出占用中）",
              pending.get("B-OLD-FILL-00000") == "导出占用中"
              and pending.get(f"B-OLD-FILL-{FILLER_ROWS - 1:05d}") == "导出占用中")
        check("未归档批次不在清理报表内", "B-LIVE" not in pending)
        check("清理后存储行数未变（边传边删没有发生）",
              len(store.rows(MODULE)) == len(seeded),
              f"当前 {len(store.rows(MODULE))} 行")

        print("== 5. 断开下载 ==")
        conn.close()
        deadline = time.time() + 10
        while export_guard.pinned_ids(MODULE) and time.time() < deadline:
            time.sleep(0.05)
        check("断开后已结束的请求释放全部占用",
              export_guard.pinned_ids(MODULE) == set(),
              f"剩余占用 {len(export_guard.pinned_ids(MODULE))} 条")

        print("== 6. 重试清理 ==")
        report_retry = cleanup_scan("CLEAN-AFTER-DISCONNECT")
        reclaimed = set(report_retry["已回收"])
        check("过期批次已回收",
              {"B-OLD-EXPIRED", "B-OLD-FILL-00000"} <= reclaimed
              and len(reclaimed) == 1 + FILLER_ROWS,
              f"已回收 {len(reclaimed)} 条")
        pending_retry = {item["批次标识"]: item["原因"] for item in report_retry["仍待回收"]}
        check("刚归档批次仍待回收（保留期内）",
              pending_retry == {"B-FRESH-KEEP": "保留期内"},
              str(pending_retry))
        remaining = {row["批次标识"] for row in store.rows(MODULE)}
        check("存储中过期批次已移除、其余保留",
              remaining == {"B-FRESH-KEEP", "B-LIVE"})

        print("== 7. 核对新文件、已收到数据、清理报表 ==")
        new_file = download_all().decode("utf-8")
        check("新文件不含已回收批次", "B-OLD-EXPIRED" not in new_file
              and "B-OLD-FILL-00000" not in new_file)
        check("新文件保留刚归档与未归档批次",
              "B-FRESH-KEEP" in new_file and "B-LIVE" in new_file)
        new_lines = new_file.splitlines(keepends=True)
        check("新文件行数 = 表头 + 剩余 2 行", len(new_lines) == 3,
              f"实际 {len(new_lines)} 行")

        expected_first = expected_csv_lines(seeded)
        received_text = received.decode("utf-8")
        received_lines = received_text.splitlines(keepends=True)
        if received_lines and not received_lines[-1].endswith("\n"):
            received_lines = received_lines[:-1]  # 最后半行是断开时截断的，不算
        check("下载确实在中途被断开（只收到前一段）",
              aborted and 2 <= len(received_lines) < len(expected_first),
              f"收到 {len(received_lines)}/{len(expected_first)} 行")
        check("已收到数据与本次结果批次逐行对上（不少行、不重复）",
              received_lines == expected_first[:len(received_lines)])
        check("收到的首行是带 BOM 的表头",
              received_lines[0].startswith("\ufeffid,批次标识"))
        check("清理报表条目已登记（已回收数/待回收数）",
              store.rows("data_cleanup")[-1]["任务标识"] == "CLEAN-AFTER-DISCONNECT"
              and store.rows("data_cleanup")[-1]["已回收数"] == str(1 + FILLER_ROWS)
              and store.rows("data_cleanup")[-1]["待回收数"] == "1")
    finally:
        server.should_exit = True

    print()
    if failures:
        print(f"结果：{len(failures)} 项未通过：{failures}")
        return 1
    print("结果：全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
