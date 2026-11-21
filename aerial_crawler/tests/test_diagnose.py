"""续采诊断的规则级单元测试（纯标准库，不启动网络服务）。

覆盖：
- start/done 配对：强杀后“有 start 无 done”的请求一律标“未知”；
- 磁盘旁证：未知请求按登记路径 stat 区分“实际已落盘 / 无证据”；
- 首次进入当前清单的运行序号与轮次（含补齐轮发布）；
- 同址换版后旧版归属（superseded）与首次发布轮次仍可查；
- 整卷复用后原归属与复用历史可回溯；
- 诊断只读：不创建目录、不获取写锁；
- RequestTracker 的尝试记录按显式 req_id 归集，不串台。
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aerial.diagnose import (
    KIND_CHUNK, KIND_IMAGE, KIND_PROBE, DiagnosticsStore, RequestTracker,
    RESULT_FAILED, RESULT_REUSED, RESULT_SERVED, STATE_UNKNOWN,
    build_report, render_text,
)
from aerial.manifest import (
    EV_DOWNLOAD_SUCCEEDED, EV_REPLACED, EV_REUSED, EV_SUPERSEDED, Manifest,
)
from aerial.parsing import VolumeDetail


def _detail(version: int, updated_at: int, checksum: str, size: int = 100) -> VolumeDetail:
    return VolumeDetail(
        volume_id="V0009", updated_at=updated_at, checksum=checksum,
        image_href="/images/V0009", size=size, version=version,
    )


class DiagnosticsStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.path = self.dir / "manifest" / "diagnostics.jsonl"
        self.store = DiagnosticsStore(self.path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_append_fsync_and_replay(self) -> None:
        self.store.append({"type": "run", "run_seq": 1, "mode": "full"})
        self.store.append({"type": "run_end", "run_seq": 1})
        recs = self.store.read_records()
        self.assertEqual([r["type"] for r in recs], ["run", "run_end"])

    def test_next_run_seq_monotonic(self) -> None:
        self.assertEqual(self.store.next_run_seq("full"), 1)
        self.store.append({"type": "run", "run_seq": 1, "mode": "full"})
        self.store.append({"type": "run", "run_seq": 3, "mode": "full"})
        self.assertEqual(self.store.next_run_seq("incremental"), 4)

    def test_truncated_tail_line_skipped(self) -> None:
        self.store.append({"type": "run", "run_seq": 1})
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write('{"type": "run", "run_seq": 2,')  # 模拟强杀留下的半行
        recs = self.store.read_records()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["run_seq"], 1)


class RequestTrackerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = DiagnosticsStore(Path(self.tmp.name) / "d.jsonl")
        self.tracker = RequestTracker(self.store, run_seq=2)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_attempts_grouped_by_explicit_req_id(self) -> None:
        rid_a = self.tracker.begin(kind=KIND_CHUNK, target="/images/x", round_no=0,
                                   volume_id="V1", version=1)
        rid_b = self.tracker.begin(kind=KIND_CHUNK, target="/images/y", round_no=0,
                                   volume_id="V2", version=1)
        # 交错回调，验证不串台
        self.tracker.note_attempt(rid_a, "/images/x", 1, 503, None, 100)
        self.tracker.note_attempt(rid_b, "/images/y", 1, 206, None, 101)
        self.tracker.note_attempt(rid_a, "/images/x", 2, 206, None, 102)
        self.tracker.end(rid_a, RESULT_SERVED, volume_id="V1", version=1)
        self.tracker.end(rid_b, RESULT_SERVED, volume_id="V2", version=1)
        recs = {r["req_id"]: r for r in self.store.read_records()
                if r.get("phase") == "done"}
        self.assertEqual([a["status"] for a in recs[rid_a]["attempts"]], [503, 206])
        self.assertEqual([a["status"] for a in recs[rid_b]["attempts"]], [206])

    def test_none_req_id_hook_ignored(self) -> None:
        # 无诊断上下文的普通 HTTP 调用不应报错
        self.tracker.note_attempt(None, "/x", 1, 200, None, 1)
        self.assertEqual(self.store.read_records(), [])


class ReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.manifest_dir = self.root / "manifest"
        self.manifest_dir.mkdir()
        self.images_dir = self.root / "images" / "V0009"
        self.images_dir.mkdir(parents=True)
        self.diag = self.manifest_dir / "diagnostics.jsonl"
        self.events = self.manifest_dir / "events.jsonl"
        self.current = self.manifest_dir / "current.json"
        self.store = DiagnosticsStore(self.diag)
        self.chunk_path = self.images_dir / "chunks" / "v1_aaa" / "chunk_000000"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_events(self, events: list[dict], run_seq: int = 1, round_no: int = 0) -> None:
        m = Manifest(self.events, self.current)
        m.run_seq = run_seq
        m.round_no = round_no
        for ev in events:
            m._append_event(dict(ev))

    def test_unknown_request_marked_and_disk_evidence_distinguished(self) -> None:
        tracker = RequestTracker(self.store, run_seq=1)
        self.store.append({"type": "run", "run_seq": 1, "mode": "full", "pid": 999999})
        # 请求 A：start 后模拟强杀，无 done；但区段实际已原子落盘
        rid_a = tracker.begin(
            kind=KIND_CHUNK, target="/images/V0009", round_no=0,
            volume_id="V0009", version=1, range_start=0, range_end=9,
            chunk_index=0, chunk_path=str(self.chunk_path), expected_len=10,
        )
        self.chunk_path.parent.mkdir(parents=True)
        self.chunk_path.write_bytes(b"0123456789")
        # 请求 B：start 后强杀，无 done 且无落盘
        rid_b = tracker.begin(
            kind=KIND_CHUNK, target="/images/V0009", round_no=0,
            volume_id="V0009", version=1, range_start=10, range_end=19,
            chunk_index=1,
            chunk_path=str(self.images_dir / "chunks" / "v1_aaa" / "chunk_000001"),
            expected_len=10,
        )
        self.assertIn(rid_a, {r.get("req_id") for r in self.store.read_records()})

        report = build_report(self.root, volume_id="V0009")
        self.assertEqual(report["runs"][0]["status"], "crashed")
        reqs = report["volumes"]["V0009"]["versions"][0]["requests"]
        by_idx = {r["chunk_index"]: r for r in reqs}
        self.assertEqual(by_idx[0]["state"], STATE_UNKNOWN)
        self.assertEqual(by_idx[0]["disk_state"], "chunk_present")
        self.assertEqual(by_idx[1]["state"], STATE_UNKNOWN)
        self.assertEqual(by_idx[1]["disk_state"], "no_evidence")
        self.assertEqual(report["inflight_unknown"], 2)
        # 文本报告包含“未知”与落盘旁证
        text = render_text(report)
        self.assertIn("未知", text)
        self.assertIn("区段文件实际存在", text)

    def test_failed_request_with_chunk_present_flagged_unexpected(self) -> None:
        tracker = RequestTracker(self.store, run_seq=1)
        rid = tracker.begin(
            kind=KIND_CHUNK, target="/images/V0009", round_no=0,
            volume_id="V0009", version=1, range_start=0, range_end=9,
            chunk_index=0, chunk_path=str(self.chunk_path), expected_len=10,
        )
        # 后续重试补齐了同路径区段，但原逻辑请求登记为失败
        self.chunk_path.parent.mkdir(parents=True)
        self.chunk_path.write_bytes(b"0123456789")
        tracker.end(rid, RESULT_FAILED, volume_id="V0009", version=1,
                    error="重试耗尽")
        report = build_report(self.root, volume_id="V0009")
        req = report["volumes"]["V0009"]["versions"][0]["requests"][0]
        self.assertEqual(req["state"], RESULT_FAILED)
        self.assertEqual(req["disk_state"], "unexpected_present")

    def test_first_publish_round_and_superseded_old_version(self) -> None:
        # run 1 主扫描发布 v1；run 2 补齐轮 1 同址换版发布 v2，旧版变 superseded
        img1 = self.images_dir / "v1_aaa.img"
        img2 = self.images_dir / "v2_bbb.img"
        img1.write_bytes(b"1")
        img2.write_bytes(b"22")
        m = Manifest(self.events, self.current)
        m.run_seq, m.round_no = 1, 0
        m.commit_image(_detail(1, 100, "aaa"), img1)
        m.run_seq, m.round_no = 2, 1
        m.commit_image(_detail(2, 200, "bbb", size=2), img2)
        # 旧版晚完成的单调保护场景
        m.run_seq, m.round_no = 2, 2
        result = m.commit_image(_detail(1, 100, "aaa"), img1)
        self.assertEqual(result, EV_SUPERSEDED)

        report = build_report(self.root, volume_id="V0009")
        v = report["volumes"]["V0009"]
        self.assertEqual(v["current_entry"]["version"], 2)
        by_ver = {x["version"]: x for x in v["versions"]}
        self.assertEqual(by_ver[1]["publish_state"], "superseded")
        self.assertEqual(by_ver[1]["first_publish"],
                         {"run_seq": 1, "round": 0,
                          "event": EV_DOWNLOAD_SUCCEEDED, "site_time": 100})
        self.assertEqual(by_ver[2]["publish_state"], "current")
        self.assertEqual(by_ver[2]["first_publish"],
                         {"run_seq": 2, "round": 1,
                          "event": EV_REPLACED, "site_time": 200})
        # 一致性核对通过
        self.assertTrue(all(c["ok"] for c in report["consistency"]))

    def test_reuse_keeps_original_attribution_and_history(self) -> None:
        img = self.images_dir / "v1_aaa.img"
        img.write_bytes(b"1")
        m = Manifest(self.events, self.current)
        m.run_seq, m.round_no = 1, 0
        m.commit_image(_detail(1, 100, "aaa"), img)
        # run 3 整卷复用
        m.run_seq, m.round_no = 3, 0
        reused = m.commit_image(_detail(1, 100, "aaa"), img, reused=True)
        self.assertEqual(reused, EV_REUSED)

        report = build_report(self.root, volume_id="V0009")
        ver = report["volumes"]["V0009"]["versions"][0]
        self.assertEqual(ver["publish_state"], "current")
        # 首次发布轮次仍是 run 1，不被复用覆盖
        self.assertEqual(ver["first_publish"]["run_seq"], 1)
        self.assertEqual(ver["reuse_history"], [
            {"run_seq": 3, "round": 0, "updated_at": 100},
        ])

    def test_served_chunk_cross_check_after_assembly(self) -> None:
        # 已提交的版本：区段目录被清理，报告应识别为“已装配提交”
        img = self.images_dir / "v1_aaa.img"
        img.write_bytes(b"1")
        m = Manifest(self.events, self.current)
        m.run_seq, m.round_no = 1, 0
        m.commit_image(_detail(1, 100, "aaa"), img)

        tracker = RequestTracker(self.store, run_seq=1)
        rid = tracker.begin(
            kind=KIND_CHUNK, target="/images/V0009", round_no=0,
            volume_id="V0009", version=1, range_start=0, range_end=9,
            chunk_index=0, chunk_path=str(self.chunk_path), expected_len=10,
        )
        tracker.end(rid, RESULT_SERVED, volume_id="V0009", version=1)
        report = build_report(self.root, volume_id="V0009")
        req = report["volumes"]["V0009"]["versions"][0]["requests"][0]
        self.assertEqual(req["state"], RESULT_SERVED)
        self.assertEqual(req["disk_state"], "assembled_committed")

    def test_build_report_is_read_only_on_missing_dir(self) -> None:
        empty = Path(self.tmp.name) / "never_created"
        report = build_report(empty)
        self.assertFalse(empty.exists())
        self.assertEqual(report["volumes"], {})
        self.assertEqual(report["runs"], [])

    def test_probe_failure_recorded_and_reused_chunk_rows(self) -> None:
        tracker = RequestTracker(self.store, run_seq=2)
        rid = tracker.begin(kind=KIND_PROBE, target="/images/V0009", round_no=1,
                            volume_id="V0009", version=1)
        tracker.end(rid, RESULT_FAILED, volume_id="V0009", version=1,
                    error="探测失败")
        tracker.record_reused(
            kind=KIND_CHUNK, target="/images/V0009", round_no=1,
            volume_id="V0009", version=1, range=[0, 9], chunk_index=0,
            chunk_path=str(self.chunk_path), expected_len=10,
        )
        report = build_report(self.root, volume_id="V0009")
        reqs = report["volumes"]["V0009"]["versions"][0]["requests"]
        kinds = {(r["kind"], r["state"]) for r in reqs}
        self.assertIn(("probe", RESULT_FAILED), kinds)
        self.assertIn((KIND_CHUNK, RESULT_REUSED), kinds)

    def test_whole_image_reuse_recorded_as_image_kind(self) -> None:
        img = self.images_dir / "v1_aaa.img"
        img.write_bytes(b"1")
        m = Manifest(self.events, self.current)
        m.run_seq, m.round_no = 1, 0
        m.commit_image(_detail(1, 100, "aaa"), img)
        tracker = RequestTracker(self.store, run_seq=4)
        tracker.record_reused(
            kind=KIND_IMAGE, target="/images/V0009", round_no=0,
            volume_id="V0009", version=1, chunk_path=str(img),
        )
        report = build_report(self.root, volume_id="V0009")
        reqs = report["volumes"]["V0009"]["versions"][0]["requests"]
        self.assertEqual([(r["kind"], r["state"]) for r in reqs],
                         [(KIND_IMAGE, RESULT_REUSED)])

    def test_missing_current_image_flagged_inconsistent(self) -> None:
        img = self.images_dir / "v1_aaa.img"
        img.write_bytes(b"1")
        m = Manifest(self.events, self.current)
        m.run_seq, m.round_no = 1, 0
        m.commit_image(_detail(1, 100, "aaa"), img)
        # 提交后影像被移走：一致性核对必须报错
        img.unlink()
        report = build_report(self.root, volume_id="V0009")
        self.assertFalse(all(c["ok"] for c in report["consistency"]))
        self.assertIn("不存在", report["consistency"][0]["detail"])

    def test_volume_filter_scopes_consistency_and_requests(self) -> None:
        other_dir = self.root / "images" / "V0010"
        other_dir.mkdir(parents=True)
        m = Manifest(self.events, self.current)
        m.run_seq, m.round_no = 1, 0
        img9 = self.images_dir / "v1_aaa.img"
        img9.write_bytes(b"1")
        m.commit_image(_detail(1, 100, "aaa"), img9)
        img10 = other_dir / "v1_ccc.img"
        img10.write_bytes(b"2")
        d2 = VolumeDetail("V0010", 101, "ccc", "/images/V0010", 1, 1)
        m.commit_image(d2, img10)
        report = build_report(self.root, volume_id="V0010")
        self.assertEqual(list(report["volumes"]), ["V0010"])
        self.assertEqual([c["volume_id"] for c in report["consistency"]], ["V0010"])
        self.assertTrue(all(c["ok"] for c in report["consistency"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
