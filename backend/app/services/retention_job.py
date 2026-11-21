"""保留清理领域服务：过期数据的合规保留、安全清理和预演确认。

接收生命周期策略的销毁阶段流转事件，执行数据保留清理操作。

清理调用链上的取数时点约定：

1. ``dry_run_cleanup`` 在预演阶段就冻结一份「清理前恢复快照」
   （``pre-cleanup-{计划id}``），快照里保存的是预演当时的计划状态和
   本轮候选行数，而不是执行后的「已清理」状态。
2. ``execute_cleanup`` 只使用预演冻结的候选集合，不再覆盖快照；
   同一计划重复提交（并发清理员、回执重送）只会回放同一份备份。
3. 清理期间到达的策略订正（例如延长保留期限）只改当前计划，
   不回写快照；重新预演时同时给出「原候选行数」和「新期限待选行数」。
"""
from __future__ import annotations

import re
import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 策略级字段：清理期间到达的订正在回滚后仍然生效，用于重新计算候选范围。
POLICY_FIELDS = ("保留天数", "清理条件", "下次执行")

SNAPSHOT_PREFIX = "pre-cleanup-"
_SNAPSHOT_RE = re.compile(r"pre-cleanup-\d+")


class RetentionJobService(BaseModuleService):
    MODULE = "retention_job"
    ENTITY = "清理计划"
    OPERATION_TERM = "校验"
    # 列表字段保持与原页面一致；预演候选行数、待选行数在备份回读接口里给出。
    LIST_FIELDS = ["计划编号", "目标资产", "保留天数", "清理条件", "预演模式", "清理行数", "备份路径", "下次执行"]
    REQUIRED_FIELDS = ["计划编号", "目标资产", "保留天数"]
    STATUS_ORDER = ["草稿", "执行中", "已清理", "跳过"]
    ACTION_RULES = {"预演清理": "执行中", "执行清理": "已清理", "跳过清理": "跳过"}
    NEGATIVE_ACTIONS = []

    # ── 备份编号 ──────────────────────────────────────────────

    @staticmethod
    def snapshot_id_for(job_id: int) -> str:
        """计划的清理前恢复快照编号（稳定编号，重复提交不会产生第二份）。"""
        return f"{SNAPSHOT_PREFIX}{job_id}"

    @classmethod
    def resolve_snapshot_id(cls, reference: str | None) -> str | None:
        """把版本标签、备份路径或旧备份编号归一到快照编号。

        历史备份可能写成 ``backup/clenaup-snap-pre-cleanup-1/`` 这类路径，
        只要其中包含 ``pre-cleanup-<计划id>`` 就复用同一个编号。
        """
        if not reference:
            return None
        match = _SNAPSHOT_RE.search(str(reference))
        if match:
            return match.group(0)
        return str(reference)

    @classmethod
    def job_id_from_snapshot(cls, reference: str | None) -> int | None:
        snapshot_id = cls.resolve_snapshot_id(reference)
        if not snapshot_id:
            return None
        try:
            return int(snapshot_id[len(SNAPSHOT_PREFIX):])
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_rows(value: Any) -> int:
        """把计划里的行数文本转成整数，'-' / 空值按 0 处理。"""
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 0

    # ── 预演 ──────────────────────────────────────────────────

    def dry_run_cleanup(
        self,
        job_id: int,
        data_rows: int,
        candidate_rows: list[Any] | None = None,
        retention_days: str | int | None = None,
    ) -> dict[str, Any]:
        """预演清理：统计候选行数，并在预演阶段写入清理前恢复快照。

        快照取数时点必须在计划状态改成「已清理」之前。首次预演会冻结
        ``status=执行中``、``清理行数=候选行数`` 的计划记录；之后再预演
        只计算当前期限下的待选范围，不覆盖原有候选集合。

        并发预演同样持 store 锁：两个清理员同时提交时只形成一份可回放
        的预演备份，后到的请求按已有备份回放。
        """
        with store.lock():
            return self._dry_run_cleanup_locked(
                job_id, data_rows, candidate_rows, retention_days
            )

    def _dry_run_cleanup_locked(
        self,
        job_id: int,
        data_rows: int,
        candidate_rows: list[Any] | None,
        retention_days: str | int | None,
    ) -> dict[str, Any]:
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "清理计划不存在"}

        snapshot_id = self.snapshot_id_for(job_id)
        frozen = store.get_snapshot_metadata(snapshot_id)
        now = _time.strftime("%Y-%m-%d %H:%M:%S")
        current_count = self._coerce_rows(data_rows)
        candidates = list(candidate_rows) if candidate_rows is not None else None

        # 首次预演：先写候选信息，再冻结快照，保证快照里的状态是"执行前预演"。
        if frozen is None:
            prior_status = job.get("status")
            if retention_days is not None:
                job["保留天数"] = str(retention_days)
            job["预演模式"] = "是"
            job["清理行数"] = str(current_count)
            job["预演编号"] = snapshot_id
            job["备份编号"] = snapshot_id
            job["预演保留天数"] = str(job.get("保留天数", ""))
            job["预演候选行数"] = str(current_count)
            job["预演时间"] = now
            if candidates is not None:
                job["预演候选集合"] = candidates
            job["status"] = "执行中"
            job["pending"] = True

            snap = store.create_snapshot(
                snapshot_id,
                [self.MODULE],
                reason="清理前恢复快照",
                row_counts={self.MODULE: current_count},
                metadata={
                    "snapshot_id": snapshot_id,
                    "job_id": job_id,
                    "asset": job.get("目标资产", ""),
                    "source": "dry_run",
                    "status_before_execution": "执行中",
                    "original_status": prior_status,
                    "candidate_count": current_count,
                    "candidate_rows": candidates,
                    "retention_days": str(job.get("保留天数", "")),
                    "captured_at": now,
                },
            )
            return {
                "ok": True,
                "job_id": job_id,
                "asset": job.get("目标资产"),
                "will_clean": current_count,
                "mode": "dry_run",
                "snapshot_id": snapshot_id,
                "backup_id": snapshot_id,
                "status_before_execution": "执行中",
                "original_candidate_count": current_count,
                "current_candidate_count": current_count,
                "预演候选行数": current_count,
                "待选行数": current_count,
                "candidate_rows": candidates,
                "replayed": False,
            }

        # 已有预演备份：这是重复回执或恢复后的再次预演，只更新当前待选范围。
        # 原候选集合并未丢失，快照也不会被这次预演覆盖。
        original_count = self._coerce_rows(frozen.get("candidate_count", job.get("预演候选行数", 0)))
        job["预演模式"] = "是"
        job["预演编号"] = snapshot_id
        job["备份编号"] = snapshot_id
        job["预演候选行数"] = str(original_count)
        job["待选行数"] = str(current_count)
        job["重新预演时间"] = now
        if retention_days is not None:
            job["保留天数"] = str(retention_days)
        job["清理行数"] = str(current_count)
        job["status"] = "执行中"
        job["pending"] = True
        job["当前保留天数"] = str(job.get("保留天数", ""))

        return {
            "ok": True,
            "job_id": job_id,
            "asset": job.get("目标资产"),
            "will_clean": current_count,
            "mode": "dry_run",
            "snapshot_id": snapshot_id,
            "backup_id": snapshot_id,
            "status_before_execution": frozen.get("status_before_execution"),
            "original_candidate_count": original_count,
            "original_candidates": frozen.get("candidate_rows"),
            "current_candidate_count": current_count,
            "current_candidates": candidates,
            "预演候选行数": original_count,
            "待选行数": current_count,
            "retention_days": str(job.get("保留天数", "")),
            "original_retention_days": frozen.get("retention_days"),
            "replayed": True,
        }

    # ── 执行 ──────────────────────────────────────────────────

    def execute_cleanup(self, job_id: int, create_backup: bool = True) -> dict[str, Any]:
        """执行清理操作并保留清理前备份（Pipeline 4 核心方法）。

        备份只读取预演阶段冻结的快照；若计划从未预演，则先在本方法里
        改写状态之前即时冻结，绝不把「已清理」状态写进备份。

        整个过程持 store 锁：两个清理员同时提交时，先到的写入唯一一份
        快照并执行，后到的只回放该快照，不会因为回执重送丢失预演候选行数。
        """
        with store.lock():
            return self._execute_cleanup_locked(job_id, create_backup)

    def _execute_cleanup_locked(self, job_id: int, create_backup: bool) -> dict[str, Any]:
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "清理计划不存在"}

        snapshot_id = self.snapshot_id_for(job_id)
        frozen = store.get_snapshot_metadata(snapshot_id)
        now = _time.strftime("%Y-%m-%d %H:%M:%S")

        # 重复提交 / 回执重送：直接回放同一份备份，不再产生第二份快照。
        # 判断依据是"本轮已执行"（备份编号挂在计划上且计划已是已清理），
        # 回滚过的计划虽然快照里留有上一轮 executed_at，仍应重新执行。
        current_backup = self.resolve_snapshot_id(job.get("备份编号") or job.get("备份路径"))
        already_executed = (
            frozen is not None
            and frozen.get("executed_at")
            and job.get("status") == "已清理"
            and current_backup == snapshot_id
        )
        if already_executed:
            return {
                "ok": True,
                "job_id": job_id,
                "asset": job.get("目标资产"),
                "candidate_count": self._coerce_rows(
                    frozen.get("last_cleaned_count", frozen.get("cleaned_count", 0))
                ),
                "cleaned": self._coerce_rows(
                    frozen.get("last_cleaned_count", frozen.get("cleaned_count", 0))
                ),
                "original_candidate_count": self._coerce_rows(frozen.get("candidate_count", 0)),
                "backup_path": job.get("备份路径"),
                "backup_id": snapshot_id,
                "replayed": True,
                "pipeline": "lifecycle_policy->retention_job",
            }

        # 未预演直接执行：在状态改写之前冻结快照，备份仍反映执行前状态。
        if create_backup and frozen is None:
            candidate_count = self._coerce_rows(job.get("清理行数"))
            store.create_snapshot(
                snapshot_id,
                [self.MODULE],
                reason="清理前恢复快照",
                row_counts={self.MODULE: candidate_count},
                metadata={
                    "snapshot_id": snapshot_id,
                    "job_id": job_id,
                    "asset": job.get("目标资产", ""),
                    "source": "execute_without_dry_run",
                    "status_before_execution": job.get("status"),
                    "original_status": job.get("status"),
                    "candidate_count": candidate_count,
                    "candidate_rows": job.get("预演候选集合"),
                    "retention_days": str(job.get("保留天数", "")),
                    "captured_at": now,
                },
            )
            frozen = store.get_snapshot_metadata(snapshot_id)

        # 本轮实际处理的行数取当前预演范围；原候选行数仍以冻结快照为准。
        # 恢复后的再次预演会把「清理行数」更新为新期限下的待选行数。
        live_count = str(job.get("清理行数", "")).strip()
        candidate_count = (
            self._coerce_rows(live_count)
            if live_count.isdigit()
            else self._coerce_rows((frozen or {}).get("candidate_count", 0))
        )
        previous_status = job.get("status")

        # 本轮实际处理的行数沿用预演候选集合，不因状态流转而丢失。
        job["清理行数"] = str(candidate_count)
        job["已清理行数"] = str(candidate_count)
        job["status"] = "已清理"
        job["pending"] = False

        if create_backup:
            job["备份编号"] = snapshot_id
            job["备份路径"] = f"backup/cleanup-snap-{snapshot_id}/"
            if frozen is not None:
                frozen["executed_at"] = now
                frozen["last_cleaned_count"] = candidate_count
                frozen.setdefault("cleaned_count", candidate_count)
                frozen["status_after_execution"] = "已清理"
                frozen["retention_days_at_execution"] = str(job.get("保留天数", ""))

        store.log_change(
            self.MODULE, job_id, "执行清理",
            before=previous_status, after="已清理",
            source="pipeline_4", scope=f"retention_job:资产{job.get('目标资产', '')}"
        )

        self.emit_event("cleanup_completed", {
            "job_id": job_id,
            "asset_code": job.get("目标资产", ""),
            "backup_path": job.get("备份路径", ""),
        })

        return {
            "ok": True,
            "job_id": job_id,
            "asset": job.get("目标资产"),
            "cleaned": candidate_count,
            "candidate_count": candidate_count,
            "original_candidate_count": self._coerce_rows(
                (frozen or {}).get("candidate_count", candidate_count)
            ),
            "backup_path": job.get("备份路径"),
            "backup_id": snapshot_id,
            "replayed": False,
            "pipeline": "lifecycle_policy->retention_job",
        }

    def skip_cleanup(self, job_id: int, reason: str) -> dict[str, Any]:
        """跳过清理（保留数据）。"""
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "清理计划不存在"}
        previous_status = job.get("status")
        job["status"] = "跳过"
        job["pending"] = False
        job["清理条件"] = f"已跳过：{reason}"
        store.log_change(
            self.MODULE, job_id, "跳过清理",
            before=previous_status, after="跳过",
            source="manual_skip", scope=f"retention_job:资产{job.get('目标资产', '')}"
        )
        return {"ok": True, "job_id": job_id, "reason": reason}

    # ── 策略订正 ──────────────────────────────────────────────

    def correct_policy(
        self, job_id: int, retention_days: str | int | None = None,
        next_run: str | None = None,
        candidate_count: int | None = None,
        candidate_rows: list[Any] | None = None,
    ) -> dict[str, Any]:
        """登记清理期间到达的策略订正；只改当前计划，不改冻结备份。

        订正后的保留期限会参与下一次预演的候选范围计算；``candidate_count``
        用于记录新期限下的待选行数。冻结快照里的原候选集合保持不变，
        恢复人员因此能同时看到原候选行数与新期限下的待选行数。
        """
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "清理计划不存在"}
        if retention_days is not None:
            job["保留天数"] = str(retention_days)
        if next_run is not None:
            job["下次执行"] = next_run
        if candidate_count is not None:
            job["待选行数"] = str(candidate_count)
        if candidate_rows is not None:
            job["待选候选集合"] = list(candidate_rows)
        job["策略订正时间"] = _time.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_id = self.snapshot_id_for(job_id)
        frozen = store.get_snapshot_metadata(snapshot_id) or {}
        store.log_change(
            self.MODULE, job_id, "策略订正",
            before=f"保留{frozen.get('retention_days', '')}天/原候选{frozen.get('candidate_count', '')}行",
            after=f"保留{job.get('保留天数', '')}天/待选{job.get('待选行数', '')}行",
            source="lifecycle_policy", scope=f"retention_job:资产{job.get('目标资产', '')}"
        )
        return {
            "ok": True,
            "job_id": job_id,
            "retention_days": str(job.get("保留天数", "")),
            "original_retention_days": frozen.get("retention_days"),
            "original_candidate_count": frozen.get("candidate_count"),
            "current_candidate_count": (
                self._coerce_rows(job.get("待选行数"))
                if job.get("待选行数") is not None else frozen.get("candidate_count")
            ),
            "snapshot_id": snapshot_id,
        }

    # ── 备份回读 / 回滚 ───────────────────────────────────────

    def get_cleanup_backup(self, job_id: int) -> dict[str, Any] | None:
        """回读计划的清理前恢复快照（状态与候选行数来自冻结副本）。"""
        snapshot_id = self.snapshot_id_for(job_id)
        data = store.get_snapshot(snapshot_id)
        if data is None:
            return None
        metadata = store.get_snapshot_metadata(snapshot_id) or {}
        rows = data.get(self.MODULE, [])
        # 快照里是整张计划表，必须按计划 id 取本计划那一行，不能固定取首行。
        frozen_job = next(
            (row for row in rows if int(row.get("id", 0)) == job_id), {}
        )
        live_job = self.get_entry(job_id) or {}
        current_count = self._coerce_rows(
            live_job.get("待选行数", live_job.get("恢复后候选行数", metadata.get("candidate_count")))
        )
        original_count = self._coerce_rows(metadata.get("candidate_count"))
        last_cleaned = metadata.get("last_cleaned_count")
        return {
            "snapshot_id": snapshot_id,
            "backup_id": snapshot_id,
            "job_id": job_id,
            # status / candidate_count 恒取自冻结副本，是执行前预演快照的原样回读。
            "status": frozen_job.get("status"),
            "status_before_execution": metadata.get("status_before_execution"),
            "清理行数": frozen_job.get("清理行数"),
            "candidate_count": original_count,
            "candidate_rows": metadata.get("candidate_rows"),
            "预演候选行数": original_count,
            # 当前计划在新期限下的待选范围，与原候选集合分开给出。
            "current_candidate_count": current_count,
            "待选行数": current_count,
            "retention_days": metadata.get("retention_days"),
            "current_retention_days": str(live_job.get("保留天数", "")),
            "captured_at": metadata.get("captured_at"),
            "last_cleaned_count": last_cleaned,
            "cleaned_count": last_cleaned if last_cleaned is not None else metadata.get("cleaned_count"),
            "version_entries": store.find_version_entries(snapshot_id),
            "rows": rows,
            "metadata": metadata,
        }

    def restore_cleanup(
        self,
        job_id: int,
        snapshot_id: str | None = None,
        candidate_rows: list[Any] | None = None,
        candidate_count: int | None = None,
    ) -> dict[str, Any]:
        """按清理前恢复快照回滚计划，并把回滚结果同步到预演清单。

        策略订正字段从回滚前的当前计划带过来，所以恢复后的候选范围反映
        最新期限；``candidate_count``/``candidate_rows`` 传入重新预演得到的
        待选集合。原清理变更记录保留在 change_capture 中仍可查询。
        """
        with store.lock():
            return self._restore_cleanup_locked(
                job_id, snapshot_id, candidate_rows, candidate_count
            )

    def _restore_cleanup_locked(
        self,
        job_id: int,
        reference: str | None,
        candidate_rows: list[Any] | None,
        candidate_count: int | None,
    ) -> dict[str, Any]:
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "清理计划不存在"}

        # 兼容已有的备份编号：版本标签、备份路径都能归一到同一快照。
        target_snapshot = (
            self.resolve_snapshot_id(reference) or self.snapshot_id_for(job_id)
        )
        data = store.get_snapshot(target_snapshot)
        if data is None:
            return {"ok": False, "error": f"备份 {target_snapshot} 不存在"}

        policy_corrections = {
            field: job.get(field) for field in POLICY_FIELDS if field in job
        }
        # 策略订正带来的新期限待选范围不属于备份冻结内容，回滚后要带走。
        pending_candidates = job.get("待选候选集合")
        pending_count = job.get("待选行数")
        original_log = store.get_change_log(self.MODULE, job_id)

        restored = store.restore_snapshot(
            target_snapshot, [self.MODULE], entry_ids=[job_id]
        )
        if restored is None:
            return {"ok": False, "error": f"备份 {target_snapshot} 无法回滚"}

        restored_job = self.get_entry(job_id)
        if restored_job is None:
            return {"ok": False, "error": "回滚副本中缺少该清理计划"}

        # 策略订正属于计划级修正，回滚后继续生效。
        applied = {}
        for field, value in policy_corrections.items():
            if value is not None and restored_job.get(field) != value:
                restored_job[field] = value
                applied[field] = value

        metadata = store.get_snapshot_metadata(target_snapshot) or {}
        original_count = self._coerce_rows(metadata.get("candidate_count"))
        if candidate_count is not None:
            current_count = self._coerce_rows(candidate_count)
        elif pending_count is not None:
            current_count = self._coerce_rows(pending_count)
        else:
            current_count = self._coerce_rows(
                restored_job.get("待选行数", original_count)
            )
        now = _time.strftime("%Y-%m-%d %H:%M:%S")

        restored_job["回滚时间"] = now
        restored_job["回滚来源编号"] = target_snapshot
        restored_job["恢复后候选行数"] = str(current_count)
        restored_job["预演候选行数"] = str(original_count)
        if candidate_rows is not None:
            restored_job["预演候选集合"] = list(candidate_rows)
        elif pending_candidates is not None:
            restored_job["预演候选集合"] = list(pending_candidates)
        if current_count != original_count:
            restored_job["待选行数"] = str(current_count)
        else:
            restored_job.pop("待选行数", None)

        # 把回滚结果同步进预演清单（管道状态），并保留原清理记录引用。
        manifest = self.pipeline_context()
        manifest.setdefault("预演清单", {})[str(job_id)] = {
            "job_id": job_id,
            "snapshot_id": target_snapshot,
            "原候选行数": original_count,
            "恢复后候选行数": current_count,
            "回滚时间": now,
            "回滚前状态": (metadata.get("status_after_execution") or job.get("status")),
            "回滚后状态": restored_job.get("status"),
            "策略订正": applied,
            "原清理记录": [
                {
                    "记录编号": record.get("记录编号"),
                    "变更类型": record.get("变更类型"),
                    "变更时间": record.get("变更时间"),
                }
                for record in original_log
            ],
        }
        self.update_pipeline_context(manifest)

        store.log_change(
            self.MODULE, job_id, "回滚恢复",
            before=job.get("status"), after=restored_job.get("status"),
            source="pre_cleanup_restore", scope=f"snapshot:{target_snapshot}"
        )

        return {
            "ok": True,
            "job_id": job_id,
            "snapshot_id": target_snapshot,
            "restored": restored.get("restored", {}),
            "status": restored_job.get("status"),
            "original_candidate_count": original_count,
            "current_candidate_count": current_count,
            "预演候选行数": original_count,
            "待选行数": current_count,
            "policy_corrections_applied": applied,
            "原清理记录": manifest["预演清单"][str(job_id)]["原清理记录"],
            "manifest": manifest["预演清单"][str(job_id)],
        }

    # 兼容不同调用方的命名。
    rollback_cleanup = restore_cleanup
    restore_from_backup = restore_cleanup
    reload_backup = get_cleanup_backup

    # ── 动作入口 ──────────────────────────────────────────────

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """把页面动作接到清理调用链，避免只改状态、不留备份。"""
        if action == "预演清理":
            job = self.get_entry(entry_id)
            if job is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            raw_count = str(job.get("清理行数", "")).strip()
            if raw_count.isdigit():
                self.dry_run_cleanup(entry_id, self._coerce_rows(raw_count))
            else:
                # 行数还没采集到：只进入预演态，不冻结 0 行的假快照。
                job["预演模式"] = "是"
                job["status"] = "执行中"
                job["pending"] = True
            return self.get_entry(entry_id), "清理计划已预演清理"
        if action == "执行清理":
            job = self.get_entry(entry_id)
            if job is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            self.execute_cleanup(entry_id)
            return self.get_entry(entry_id), "清理计划已执行清理"
        if action == "跳过清理":
            job = self.get_entry(entry_id)
            if job is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            self.skip_cleanup(entry_id, "手动跳过")
            return self.get_entry(entry_id), "清理计划已跳过清理"
        return super().run_action(entry_id, action)
