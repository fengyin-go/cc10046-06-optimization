"""导出占用登记：CSV 导出传输期间保护涉及的批次不被清理回收。

导出开始时按筛选快照登记涉及的条目 id，传输结束（或客户端断开、
生成器被关闭）后释放。清理任务据此把仍在传输的批次标记为
「仍待回收」，避免边传边删导致交付中的 CSV 中途少行。
"""
from __future__ import annotations

import threading
from typing import Iterable

# token 类型：(模块 key, 该次导出登记的条目 id 元组)
HoldToken = tuple[str, tuple[int, ...]]


class ExportGuard:
    """按模块记录正在导出的条目 id，支持并发导出引用计数。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # module -> {entry_id: 占用计数}
        self._holds: dict[str, dict[int, int]] = {}

    def acquire(self, module: str, entry_ids: Iterable[int]) -> HoldToken:
        """登记一批条目 id 为导出占用中，返回释放用的 token。"""
        ids = tuple(dict.fromkeys(int(entry_id) for entry_id in entry_ids))
        with self._lock:
            counts = self._holds.setdefault(module, {})
            for entry_id in ids:
                counts[entry_id] = counts.get(entry_id, 0) + 1
        return (module, ids)

    def release(self, token: HoldToken) -> None:
        """释放一次导出占用；同一 id 被多次占用时按计数递减。"""
        module, ids = token
        with self._lock:
            counts = self._holds.get(module)
            if not counts:
                return
            for entry_id in ids:
                remaining = counts.get(entry_id, 0) - 1
                if remaining > 0:
                    counts[entry_id] = remaining
                else:
                    counts.pop(entry_id, None)
            if not counts:
                self._holds.pop(module, None)

    def pinned_ids(self, module: str) -> set[int]:
        """当前仍被导出占用的条目 id 集合。"""
        with self._lock:
            return set(self._holds.get(module, {}))

    def is_pinned(self, module: str, entry_id: int) -> bool:
        with self._lock:
            return int(entry_id) in self._holds.get(module, {})


export_guard = ExportGuard()
