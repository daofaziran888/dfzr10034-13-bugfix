"""清理备份回读回归测试：预演快照取数时点、幂等回放、策略订正与恢复范围。

这些用例覆盖保留清理调用链上容易出错的三处：
- 备份必须在预演阶段取数，不能把「已清理」状态写进快照；
- 并发清理员 / 回执重送只形成一份可回放的预演备份；
- 策略订正只改当前计划，恢复后仍能区分原候选集合与新期限待选集合。
"""
import threading

import app.store as store_mod
from app.services.lifecycle_policy import LifecyclePolicyService
from app.services.retention_job import RetentionJobService
from app.services.version_history import VersionHistoryService

JOB_ID = 1
ORIGINAL_ROWS = [f"order-{idx:03d}" for idx in range(8)]
NEW_ROWS = [f"order-{idx:03d}" for idx in range(5)]


def _fresh_services():
    store_mod.store.reset()
    return RetentionJobService(), VersionHistoryService()


def _frozen_job():
    return store_mod.store.get_snapshot("pre-cleanup-1")["retention_job"][0]


def _version_entries(snapshot_id="pre-cleanup-1"):
    return store_mod.store.find_version_entries(snapshot_id)


# ══════════════════════════════════════════════════════════════════
# 备份取数时点：预演前状态，而非执行后状态
# ══════════════════════════════════════════════════════════════════

class TestBackupCapturePoint:

    def test_dry_run_writes_recovery_snapshot_before_execution(self):
        cleanup, _ = _fresh_services()
        result = cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)

        assert result["ok"] is True
        assert result["snapshot_id"] == "pre-cleanup-1"
        frozen = _frozen_job()
        assert frozen["status"] == "执行中"
        assert frozen["清理行数"] == "8"

    def test_execute_does_not_overwrite_snapshot_with_cleaned_status(self):
        cleanup, _ = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(JOB_ID)

        frozen = _frozen_job()
        assert frozen["status"] == "执行中"
        assert frozen["清理行数"] == "8"
        assert store_mod.store.find("retention_job", JOB_ID)["status"] == "已清理"

    def test_execute_without_dry_run_still_captures_pre_status(self):
        cleanup, _ = _fresh_services()
        job = store_mod.store.find("retention_job", JOB_ID)
        job["清理行数"] = "8"
        job["status"] = "草稿"

        cleanup.execute_cleanup(JOB_ID)

        frozen = _frozen_job()
        assert frozen["status"] == "草稿"
        metadata = store_mod.store.get_snapshot_metadata("pre-cleanup-1")
        assert metadata["status_before_execution"] == "草稿"
        assert metadata["candidate_count"] == 8

    def test_version_entry_records_candidate_row_count(self):
        cleanup, _ = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)

        entries = _version_entries()
        assert len(entries) == 1
        assert entries[0]["行数记录"] == "8"
        assert entries[0]["创建原因"] == "清理前恢复快照"


# ══════════════════════════════════════════════════════════════════
# 同一计划一份备份：并发提交 / 回执重送
# ══════════════════════════════════════════════════════════════════

class TestSingleReplayableBackup:

    def test_two_cleaners_dry_run_only_one_snapshot(self):
        cleanup, _ = _fresh_services()
        barrier = threading.Barrier(2)
        results = []

        def worker():
            barrier.wait()
            results.append(cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(store_mod.store._snapshots) == 1
        assert len(_version_entries()) == 1
        assert sorted(item["replayed"] for item in results) == [False, True]
        assert all(item["original_candidate_count"] == 8 for item in results)

    def test_duplicate_execute_replays_instead_of_re_snapshotting(self):
        cleanup, _ = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        first = cleanup.execute_cleanup(JOB_ID)
        second = cleanup.execute_cleanup(JOB_ID)

        assert first["replayed"] is False
        assert second["replayed"] is True
        assert second["cleaned"] == 8
        assert len(store_mod.store._snapshots) == 1
        assert len(_version_entries()) == 1

    def test_concurrent_execute_keeps_candidate_rows(self):
        cleanup, _ = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        barrier = threading.Barrier(2)
        results = []

        def worker():
            barrier.wait()
            results.append(cleanup.execute_cleanup(JOB_ID))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sorted(item["replayed"] for item in results) == [False, True]
        assert all(item["cleaned"] == 8 for item in results)
        assert len(store_mod.store._snapshots) == 1


# ══════════════════════════════════════════════════════════════════
# 策略订正：原候选集合 vs 新期限待选集合
# ══════════════════════════════════════════════════════════════════

class TestPolicyCorrectionAfterCleanup:

    def test_lifecycle_revision_propagates_to_cleanup_job(self):
        """生命周期策略订正通过调用链下发到清理计划，但不改冻结备份。"""
        store_mod.store.reset()
        lifecycle = LifecyclePolicyService()
        policy, _ = lifecycle.create_entry({
            "策略编号": "LIFE-REV", "目标资产": "订单表", "创建阶段": "创建阶段",
        })
        triggered = lifecycle.transition_stage(policy["id"], "销毁阶段")
        job_id = triggered["retention_cleanup"]["id"]

        cleanup = RetentionJobService()
        cleanup.dry_run_cleanup(job_id, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(job_id)
        result = lifecycle.revise_retention(policy["id"], "365", candidate_count=5)

        assert result["ok"] is True
        assert len(result["updated_jobs"]) == 1
        job = store_mod.store.find("retention_job", job_id)
        assert job["保留天数"] == "365"
        assert job["预演候选行数"] == "8"
        assert job["待选行数"] == "5"
        # 冻结快照仍是订正前的预演保留期限
        metadata = store_mod.store.get_snapshot_metadata(f"pre-cleanup-{job_id}")
        assert metadata["retention_days"] == "730"
        assert metadata["candidate_count"] == 8

    def _run_cleanup(self):
        cleanup, versions = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(JOB_ID)
        cleanup.correct_policy(JOB_ID, retention_days="365", candidate_count=5,
                               candidate_rows=NEW_ROWS)
        return cleanup, versions

    def test_backup_reload_distinguishes_original_and_current_candidates(self):
        cleanup, _ = self._run_cleanup()
        backup = cleanup.get_cleanup_backup(JOB_ID)

        assert backup["status"] == "执行中"
        assert backup["candidate_count"] == 8
        assert backup["candidate_rows"] == ORIGINAL_ROWS
        assert backup["current_candidate_count"] == 5
        assert backup["retention_days"] == "90"
        assert backup["current_retention_days"] == "365"

    def test_second_dry_run_reports_both_candidate_sets(self):
        cleanup, _ = self._run_cleanup()
        result = cleanup.dry_run_cleanup(JOB_ID, 5, candidate_rows=NEW_ROWS)

        assert result["replayed"] is True
        assert result["original_candidate_count"] == 8
        assert result["original_candidates"] == ORIGINAL_ROWS
        assert result["current_candidate_count"] == 5
        assert result["current_candidates"] == NEW_ROWS
        # 再次预演不能改写冻结的候选集合
        assert _frozen_job()["清理行数"] == "8"
        assert store_mod.store.get_snapshot_metadata("pre-cleanup-1")["retention_days"] == "90"

    def test_execute_after_restore_is_not_treated_as_receipt_replay(self):
        cleanup, _ = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(JOB_ID)
        cleanup.restore_cleanup(JOB_ID)

        # 回滚后按新期限再次预演，再执行应真正执行本轮 5 行，而不是回放上一轮。
        cleanup.dry_run_cleanup(JOB_ID, 5, candidate_rows=NEW_ROWS)
        result = cleanup.execute_cleanup(JOB_ID)

        assert result["replayed"] is False
        assert result["cleaned"] == 5
        assert result["original_candidate_count"] == 8
        backup = cleanup.get_cleanup_backup(JOB_ID)
        assert backup["candidate_count"] == 8      # 备份仍是原预演候选
        assert backup["last_cleaned_count"] == 5   # 最近一轮实际处理 5 行

    def test_restore_keeps_policy_correction_and_original_record(self):
        cleanup, _ = self._run_cleanup()
        result = cleanup.restore_cleanup(JOB_ID)

        assert result["ok"] is True
        assert result["status"] == "执行中"
        assert result["original_candidate_count"] == 8
        assert result["current_candidate_count"] == 5
        job = store_mod.store.find("retention_job", JOB_ID)
        assert job["保留天数"] == "365"
        assert job["预演候选行数"] == "8"
        assert job["恢复后候选行数"] == "5"
        # 原清理记录仍能查到
        assert any(item["变更类型"] == "执行清理" for item in result["原清理记录"])

    def test_restore_syncs_manifest_with_original_record(self):
        cleanup, _ = self._run_cleanup()
        cleanup.restore_cleanup(JOB_ID)

        manifest = cleanup.pipeline_context()["预演清单"]["1"]
        assert manifest["原候选行数"] == 8
        assert manifest["恢复后候选行数"] == 5
        assert manifest["回滚后状态"] == "执行中"
        assert manifest["原清理记录"]
        assert any(row["变更类型"] == "执行清理"
                   for row in store_mod.store.get_change_log("retention_job", JOB_ID))


# ══════════════════════════════════════════════════════════════════
# 版本履历：回读原编号，兼容旧备份编号
# ══════════════════════════════════════════════════════════════════

class TestVersionHistoryReadback:

    def test_snapshot_detail_reads_frozen_state_and_candidate_rows(self):
        cleanup, versions = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(JOB_ID)

        detail = versions.get_snapshot_detail("pre-cleanup-1")
        assert detail is not None
        assert detail["status"] == "执行中"
        assert detail["status_before_execution"] == "执行中"
        assert detail["candidate_count"] == 8
        assert detail["清理行数"] == "8"
        assert detail["version"]["快照编号"] == "pre-cleanup-1-retention_job"

    def test_existing_backup_id_is_reused_not_duplicated(self):
        cleanup, versions = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)

        result = versions.create_version("retention_job", "pre-cleanup-1", "重复登记")
        assert result["replayed"] is True
        assert len(_version_entries()) == 1

    def test_legacy_backup_path_resolves_to_same_snapshot(self):
        cleanup, versions = _fresh_services()
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(JOB_ID)

        legacy = "backup/clenaup-snap-pre-cleanup-1/"
        assert RetentionJobService.resolve_snapshot_id(legacy) == "pre-cleanup-1"
        result = cleanup.restore_cleanup(JOB_ID, snapshot_id=legacy)
        assert result["ok"] is True
        assert result["snapshot_id"] == "pre-cleanup-1"

    def test_restore_only_touches_the_given_plan(self):
        cleanup, _ = _fresh_services()
        store_mod.store.rows("retention_job").append({
            "id": 2, "status": "草稿", "pending": True, "abnormal": False,
            "计划编号": "RETE-0002", "目标资产": "另一张表", "保留天数": "30",
            "清理条件": "-", "预演模式": "否", "清理行数": "-",
            "备份路径": "-", "下次执行": "-",
        })
        cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        cleanup.execute_cleanup(JOB_ID)
        cleanup.restore_cleanup(JOB_ID)

        other = store_mod.store.find("retention_job", 2)
        assert other["计划编号"] == "RETE-0002"
        assert other["status"] == "草稿"


# ══════════════════════════════════════════════════════════════════
# 完整走查：先预演八行 → 执行带备份 → 版本列表 → 回读 → 再预演
# ══════════════════════════════════════════════════════════════════

class TestEndToEndCleanupRestore:

    def test_dry_run_selects_eight_rows_then_restore_scope(self):
        cleanup, versions = _fresh_services()

        # 1. 预演八行
        planned = cleanup.dry_run_cleanup(JOB_ID, 8, candidate_rows=ORIGINAL_ROWS)
        assert planned["will_clean"] == 8
        backup_id = planned["backup_id"]

        # 2. 带备份执行清理
        executed = cleanup.execute_cleanup(JOB_ID)
        assert executed["cleaned"] == 8
        assert executed["backup_path"].endswith(f"{backup_id}/")

        # 3. 在版本列表按原编号找回
        entries = _version_entries(backup_id)
        assert len(entries) == 1
        assert entries[0]["行数记录"] == "8"

        # 4. 从快照详情回读状态与候选行数
        detail = versions.get_snapshot_detail(backup_id)
        assert detail["status"] == "执行中"
        assert detail["candidate_count"] == 8

        # 5. 清理后延长保留期限，候选范围随之缩小
        cleanup.correct_policy(JOB_ID, retention_days="365", candidate_count=5,
                               candidate_rows=NEW_ROWS)
        cleanup.restore_cleanup(JOB_ID)

        # 6. 再次预演核对恢复范围：8 行原候选，5 行新期限待选
        again = cleanup.dry_run_cleanup(JOB_ID, 5, candidate_rows=NEW_ROWS)
        assert again["original_candidate_count"] == 8
        assert again["current_candidate_count"] == 5
        job = store_mod.store.find("retention_job", JOB_ID)
        assert job["status"] == "执行中"
        assert job["预演候选行数"] == "8"
        assert job["待选行数"] == "5"
