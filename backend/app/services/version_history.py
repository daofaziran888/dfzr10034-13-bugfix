"""版本履历领域服务：数据资产的多版本Schema快照、差异对比和回查能力。

维护跨时间点的数据资产版本快照，支持回溯和差异分析。
"""
from __future__ import annotations

import re
import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 旧备份路径可能写成 backup/clenaup-snap-pre-cleanup-1/，回读时归一到快照编号。
_SNAPSHOT_RE = re.compile(r"pre-cleanup-\d+")


class VersionHistoryService(BaseModuleService):
    MODULE = "version_history"
    ENTITY = "版本快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "目标资产", "版本标签", "Schema摘要", "行数记录", "创建原因", "差异字段", "快照时间"]
    REQUIRED_FIELDS = ["快照编号", "目标资产", "版本标签"]
    STATUS_ORDER = ["当前版本", "历史版本", "已标注"]
    ACTION_RULES = {"创建快照": "历史版本", "标注版本": "已标注"}
    NEGATIVE_ACTIONS = []

    def create_version(self, asset_code: str, version_tag: str, reason: str, schema_summary: str = "",
                       row_count: str = "0", diff_fields: str = "") -> dict[str, Any]:
        """创建资产版本快照。

        版本标签可能直接沿用已有备份编号（例如 ``pre-cleanup-1``）：
        此时不再追加重复记录，而是复用版本履历里的原编号，保证回读时
        能找到同一份清理前快照。
        """
        existing = self.find_version(version_tag)
        if existing is not None:
            return {"version": existing, "store_snapshot": None, "replayed": True}

        snap_id = store.next_id(self.MODULE)
        snapshot = {
            "id": snap_id,
            "快照编号": f"VERS-{snap_id:04d}",
            "目标资产": asset_code,
            "版本标签": version_tag,
            "Schema摘要": schema_summary or f"资产{asset_code}快照",
            "行数记录": row_count,
            "创建原因": reason,
            "差异字段": diff_fields,
            "快照时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "历史版本",
            "pending": False,
            "abnormal": False,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(snapshot)

        # 同时使用 store 级的快照功能保存完整数据副本
        store_snapshot = store.create_snapshot(f"version-{version_tag}-{asset_code}")

        return {
            "version": snapshot,
            "store_snapshot": store_snapshot,
            "replayed": False,
        }

    def find_version(self, snapshot_ref: str) -> dict[str, Any] | None:
        """按版本标签、快照编号或记录 id 定位版本记录，兼容已有的备份编号。"""
        for row in store.rows(self.MODULE):
            if snapshot_ref in (str(row.get("版本标签", "")), str(row.get("快照编号", "")), str(row.get("id", ""))):
                return row
        return None

    def get_snapshot_detail(self, snapshot_ref: str) -> dict[str, Any] | None:
        """回读快照详情：状态与候选行数取自清理前冻结副本，而非当前计划状态。

        ``snapshot_ref`` 可以是版本标签、快照编号，也可以是历史备份路径。
        """
        match = _SNAPSHOT_RE.search(str(snapshot_ref))
        if match:
            snapshot_ref = match.group(0)
        version = self.find_version(snapshot_ref)
        version_entries = store.find_version_entries(snapshot_ref)
        metadata = store.get_snapshot_metadata(snapshot_ref)
        data = store.get_snapshot(snapshot_ref)

        # 没有以该编号命名的 store 快照时，退回版本履历里登记的编号。
        if data is None and version is not None:
            for candidate in (str(version.get("版本标签", "")), str(version.get("快照编号", ""))):
                data = store.get_snapshot(candidate)
                if data is not None:
                    snapshot_ref = candidate
                    metadata = store.get_snapshot_metadata(candidate)
                    break

        if data is None and version is None:
            return None

        frozen_rows = (data or {}).get(self.MODULE) or (data or {}).get("retention_job") or []
        metadata = metadata or {}
        # 快照按整表冻结，详情要定位到该备份对应的计划行。
        target_job_id = metadata.get("job_id")
        frozen_job = {}
        if frozen_rows:
            frozen_job = next(
                (row for row in frozen_rows if int(row.get("id", 0)) == target_job_id),
                frozen_rows[0],
            )
        if not metadata and frozen_job:
            # 兼容没有元数据的旧快照：直接从冻结行里读候选行数。
            metadata = {
                "status_before_execution": frozen_job.get("status"),
                "candidate_count": frozen_job.get("清理行数"),
            }

        return {
            "snapshot_id": snapshot_ref,
            "version": version,
            "version_entries": version_entries,
            "status": frozen_job.get("status"),
            "status_before_execution": metadata.get("status_before_execution"),
            "清理行数": frozen_job.get("清理行数"),
            "candidate_count": metadata.get("candidate_count"),
            "candidate_rows": metadata.get("candidate_rows"),
            "预演候选行数": metadata.get("candidate_count"),
            "retention_days": metadata.get("retention_days"),
            "captured_at": metadata.get("captured_at"),
            "executed_at": metadata.get("executed_at"),
            "last_cleaned_count": metadata.get("last_cleaned_count"),
            "cleaned_count": (
                metadata.get("last_cleaned_count")
                if metadata.get("last_cleaned_count") is not None
                else metadata.get("cleaned_count")
            ),
            "metadata": metadata,
            "rows": frozen_rows,
            "job": frozen_job,
        }

    def annotate_version(self, snapshot_id: int, annotation: str) -> dict[str, Any] | None:
        """标注版本快照。"""
        entry, msg = self.run_action(snapshot_id, "标注版本")
        if entry:
            entry["标注"] = annotation
            entry["标注时间"] = _time.strftime("%Y-%m-%d %H:%M:%S")
        return entry

    def get_versions_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有版本快照（按时间倒序）。"""
        versions = store.find_by_field(self.MODULE, "目标资产", asset_code)
        versions.sort(key=lambda v: v.get("快照时间", ""), reverse=True)
        return versions

    def get_current_version(self, asset_code: str) -> dict[str, Any] | None:
        """获取资产的当前版本快照。"""
        versions = self.get_versions_by_asset(asset_code)
        return versions[0] if versions else None