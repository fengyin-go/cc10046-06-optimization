"""数据清理领域服务：按归档批次的保留期限回收存储空间，并输出清理报表。

清理报表对照批次目录 CSV 的交付范围，逐条注明哪些记录已回收、
哪些仍待回收（保留期内或正被导出占用），值班员据此安排下一次清理。
"""
from __future__ import annotations

import time
from datetime import datetime

from app.services.base import BaseModuleService
from app.services.export_guard import export_guard
from app.store import store

# 可回收的终态：归档/停用/过期/废弃。
ARCHIVED_STATUSES = ("已归档", "已停用", "已过期", "已废弃")
# 批次日期字段，按优先级取第一个能解析的。
BATCH_DATE_FIELDS = ("归档时间", "采集时间")
BATCH_DATE_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")


def _batch_date(row: dict) -> datetime | None:
    """从批次行里解析保留期限的起算日期。"""
    for field in BATCH_DATE_FIELDS:
        raw = str(row.get(field) or "").strip()
        if not raw:
            continue
        for fmt in BATCH_DATE_FORMATS:
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
    return None


class DataCleanupService(BaseModuleService):
    MODULE = "data_cleanup"
    ENTITY = "清理任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务标识", "目标模块", "保留天数", "清理类型", "预计影响", "执行状态", "上次执行", "下次执行"]
    REQUIRED_FIELDS = ["任务标识", "目标模块", "保留天数"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"启动清理": "待执行", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []

    def domain_cleanup(self, task_id: str, target_module: str,
                       retention_days: int = 30, cleanup_type: str = "soft") -> dict:
        """按保留期限回收归档批次，返回区分已回收/仍待回收的清理报表。

        - 已到期的归档批次：从目标模块移除，计入「已回收」；
        - 已到期但正被导出占用：保留原样，计入「仍待回收」（导出占用中）；
        - 未到保留期限：保留原样，计入「仍待回收」（保留期内）。
        """
        rows = store.rows(self.MODULE)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        target_rows = store.rows(target_module)
        pinned = export_guard.pinned_ids(target_module)
        now_dt = datetime.now()

        reclaimed: list[dict] = []
        pending: list[dict] = []
        reclaimed_ids: set[int] = set()
        for row in list(target_rows):
            if row.get("status") not in ARCHIVED_STATUSES:
                continue
            entry_id = int(row.get("id", 0))
            label = str(row.get("批次标识") or entry_id)
            batch_date = _batch_date(row)
            if batch_date is None:
                pending.append({"批次标识": label, "原因": "批次时间无法解析"})
                continue
            if (now_dt - batch_date).days < retention_days:
                pending.append({"批次标识": label, "原因": "保留期内"})
                continue
            if entry_id in pinned:
                # 已到保留阈值但仍在交付传输中：本次不回收，等下一趟。
                pending.append({"批次标识": label, "原因": "导出占用中"})
                continue
            reclaimed.append({"批次标识": label})
            reclaimed_ids.add(entry_id)

        if reclaimed_ids:
            target_rows[:] = [
                row for row in target_rows
                if int(row.get("id", 0)) not in reclaimed_ids
            ]

        entry = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "任务标识": task_id,
            "目标模块": target_module,
            "保留天数": str(retention_days),
            "清理类型": cleanup_type,
            "预计影响": str(len(reclaimed) + len(pending)),
            "执行状态": "已完成",
            "上次执行": now,
            "下次执行": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 86400)),
            "已回收数": str(len(reclaimed)),
            "待回收数": str(len(pending)),
            "status": "已完成",
            "pending": False,
            "abnormal": False,
        }
        rows.append(entry)
        # 报表在任务条目的基础上附上批次级明细，供值班员对照批次目录
        # CSV 逐条核对；明细只放在本次返回里，不随条目长期占用内存。
        report = dict(entry)
        report["已回收"] = [item["批次标识"] for item in reclaimed]
        report["仍待回收"] = pending
        return report
