"""规则级单元测试（不启动网络服务，纯标准库 unittest）。

覆盖：
- 清单单调保护：较晚完成的旧版下载不能让清单回退；
- 同一卷同一版本重复提交只保留一份当前条目；
- 补登地平线 / 增量候选判定；
- 限流器的最小间隔与统一暂停；
- HTML 目录（含重复链接）与详情解析；
- 分段校验：区段复用与校验失败行为。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aerial.config import Config
from aerial.limiter import RateLimiter
from aerial.manifest import (
    EV_DOWNLOAD_SUCCEEDED, EV_REPLACED, EV_SUPERSEDED, Manifest,
)
from aerial.parsing import CatalogEntry, parse_catalog, parse_detail
from aerial.scanner import Scanner
from aerial.state import WorkDir
from mocksite.scenario import Scenario, build_volumes


def make_config(workdir: Path) -> Config:
    return Config(base_url="http://127.0.0.1:8080", workdir=workdir,
                  backfill_seconds=600)


class ManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.wd = Path(self.tmp.name)
        self.manifest = Manifest(self.wd / "events.jsonl", self.wd / "current.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _detail(self, version: int, updated_at: int, checksum: str = None):
        from aerial.parsing import VolumeDetail
        checksum = checksum or f"sha-{version}"
        return VolumeDetail(
            volume_id="V1", updated_at=updated_at, checksum=checksum,
            image_href="/images/V1", size=10, version=version,
        )

    def test_no_regression_when_older_version_finishes_late(self) -> None:
        d1 = self._detail(version=1, updated_at=100, checksum="aaa")
        d2 = self._detail(version=2, updated_at=200, checksum="bbb")
        img1 = self.wd / "v1.img"
        img2 = self.wd / "v2.img"
        img1.write_bytes(b"1")
        img2.write_bytes(b"2")

        # 新版先完成入库
        r2 = self.manifest.commit_image(d2, img2)
        self.assertEqual(r2, EV_DOWNLOAD_SUCCEEDED)
        # 旧版较晚才完成：必须被标记 superseded，当前条目不回退
        r1 = self.manifest.commit_image(d1, img1)
        self.assertEqual(r1, EV_SUPERSEDED)
        current = self.manifest.current_entry("V1")
        self.assertEqual(current["version"], 2)
        self.assertEqual(current["checksum"], "bbb")

        # 事件流里必须留下 superseded 痕迹
        events = [json.loads(line) for line in
                  (self.wd / "events.jsonl").read_text().splitlines()]
        self.assertIn("superseded", [e["event"] for e in events])

    def test_duplicate_same_version_single_current_entry(self) -> None:
        d1 = self._detail(version=1, updated_at=100, checksum="aaa")
        img = self.wd / "v1.img"
        img.write_bytes(b"1")
        self.manifest.commit_image(d1, img)
        self.manifest.commit_image(d1, img)  # 重复链接触发的重复提交
        self.assertEqual(self.manifest.volume_count(), 1)
        self.assertEqual(self.manifest.current_entry("V1")["version"], 1)

    def test_replace_only_after_new_commit(self) -> None:
        d1 = self._detail(version=1, updated_at=100, checksum="aaa")
        d2 = self._detail(version=2, updated_at=300, checksum="ccc")
        img1 = self.wd / "v1.img"
        img2 = self.wd / "v2.img"
        img1.write_bytes(b"1")
        img2.write_bytes(b"2")
        self.manifest.commit_image(d1, img1)
        result = self.manifest.commit_image(d2, img2)
        self.assertEqual(result, EV_REPLACED)
        entry = self.manifest.current_entry("V1")
        self.assertEqual(entry["version"], 2)
        # 旧件文件仍保留可查
        self.assertTrue(img1.exists())


class IncrementalHorizonTests(unittest.TestCase):
    def _scanner(self, mode: str, known: set[str], last_time: int | None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        wd = WorkDir(Path(tmp.name))
        sc = Scanner(make_config(Path(tmp.name)), wd, mode)
        sc.state.known_volumes = set(known)
        sc.state.last_scan_site_time = last_time
        return sc

    def test_incremental_filters_known_old_and_unknown_beyond_horizon(self) -> None:
        sc = self._scanner("incremental", known={"V0001"}, last_time=1000)
        horizon = 1100 - 600  # 站点时间 1100 时地平线 = 500
        accepted: list[str] = []

        def feed(vid: str, updated_at: int, known: bool):
            entry = CatalogEntry(vid, f"/volumes/{vid}", updated_at)
            if known:
                sc.state.known_volumes.add(vid)
            sc._consider_entry(entry, horizon)
            accepted_vids = {k[0] for k in sc._seen_catalog}
            if vid in accepted_vids:
                accepted.append(vid)

        feed("V0001", 900, known=True)    # 已知且 <= 水位：跳过
        feed("V0001", 1050, known=True)   # 已知且 > 水位：接受（换版）
        feed("V9999", 400, known=False)   # 未知且在地平线之外：跳过
        feed("V9998", 550, known=False)   # 未知但在地平线内：接受（可能补登）
        self.assertEqual(accepted, ["V0001", "V9998"])

    def test_full_mode_accepts_everything_once(self) -> None:
        sc = self._scanner("full", known=set(), last_time=None)
        for _ in range(3):
            sc._consider_entry(CatalogEntry("V0003", "/volumes/V0003", 120), None)
        # 同卷同版本（相同 updated_at）的重复链接只收一次
        self.assertEqual(len(sc._seen_catalog), 1)

    def test_revision_with_new_updated_at_re_enqueued(self) -> None:
        sc = self._scanner("full", known=set(), last_time=None)
        sc._consider_entry(CatalogEntry("V0006", "/volumes/V0006", 150), None)
        # 换版：同卷宗、更大的 updated_at，必须作为新任务再次入队
        sc._consider_entry(CatalogEntry("V0006", "/volumes/V0006", 700), None)
        self.assertEqual(len(sc._seen_catalog), 2)
        self.assertEqual(sorted(u for _, u in sc._seen_catalog), [150, 700])

    def test_empty_catalog_keeps_waiting_until_backfill_window_passes(self) -> None:
        sc = self._scanner("full", known=set(), last_time=None)
        # 未观察到空目录：不等待
        self.assertFalse(sc._should_keep_waiting())
        # 站点时间 1000 时看到空目录
        sc._catalog_was_empty = True
        sc._empty_seen_site_time = 1000
        sc._last_recheck_site_time = 1000
        self.assertTrue(sc._should_keep_waiting())
        # 窗口（600s）未走完：仍等待
        sc._last_recheck_site_time = 1599
        self.assertTrue(sc._should_keep_waiting())
        # 窗口恰好走完的边界：地平线 <= 空目录时刻，仍保守等待一轮
        sc._last_recheck_site_time = 1600
        self.assertTrue(sc._should_keep_waiting())
        # 窗口走完后：停止等待
        sc._last_recheck_site_time = 1601
        self.assertFalse(sc._should_keep_waiting())


class RateLimiterTests(unittest.TestCase):
    def test_min_interval_between_dispatches(self) -> None:
        rl = RateLimiter(0.05)
        t1 = rl.acquire()
        t0 = time.monotonic()
        t2 = rl.acquire()
        self.assertGreaterEqual(time.monotonic() - t0, 0.045)
        self.assertEqual(rl.dispatched, 2)
        # 发出时刻之差满足最小间隔（以记录在票号上的实际发出时刻为准）
        self.assertGreaterEqual(t2.sent_at - t1.sent_at, 0.045)
        rl.release(t1)
        rl.release(t2)

    def test_pause_holds_all_acquirers(self) -> None:
        rl = RateLimiter(0.0)
        rl.pause(0.2, "test")
        t0 = time.monotonic()
        ticket = rl.acquire()
        self.assertGreaterEqual(time.monotonic() - t0, 0.18)
        self.assertEqual(rl.pause_events, 1)
        self.assertTrue(ticket.paused)
        rl.release(ticket)

    def test_fifo_order_and_ticket_lifecycle(self) -> None:
        rl = RateLimiter(0.03)
        tickets = [rl.reserve() for _ in range(4)]
        # 票号按取票顺序排队
        self.assertEqual([t.idx for t in tickets], [0, 1, 2, 3])
        sent_order = []
        for t in tickets:
            rl.wait_turn(t)
            sent_order.append(t.idx)
            rl.release(t)
        self.assertEqual(sent_order, [0, 1, 2, 3])

    def test_slow_inflight_does_not_block_queue_head(self) -> None:
        """票号发头即离队：慢响应体阶段不挡后续已就绪请求。"""
        rl = RateLimiter(0.02)
        first = rl.reserve()
        rl.wait_turn(first)  # 已发出但故意不 release，模拟慢响应体在途
        second = rl.reserve()
        t0 = time.monotonic()
        rl.wait_turn(second)  # 只需等一个最小间隔，而非等 first 结束
        waited = time.monotonic() - t0
        self.assertLess(waited, 0.1)
        # 两张票都已发出在途（first 模拟慢响应体仍未结束），等待队列为空
        snap = rl.snapshot()
        self.assertEqual(snap["inflight"], 2)
        self.assertEqual(snap["waiting"], 0)
        rl.release(first)
        rl.release(second)

    def test_connect_phase_reservation_does_not_dispatch(self) -> None:
        """慢建连期间票号只预约未发出：dispatched 不增加，队首不变。"""
        rl = RateLimiter(0.0)
        ticket = rl.reserve()
        self.assertEqual(rl.dispatched, 0)
        self.assertIsNone(ticket.sent_at)
        self.assertEqual(rl.snapshot()["waiting"], 1)
        rl.release(ticket)
        self.assertEqual(rl.snapshot()["waiting"], 0)


class ParsingTests(unittest.TestCase):
    def test_parse_catalog_with_duplicates_and_next_page(self) -> None:
        html = """
        <html><body><ul>
          <li><a class="entry-link" href="/volumes/V0003"
                data-volume-id="V0003" data-updated-at="120">A</a></li>
          <li><a class="entry-link" href="/volumes/V0003"
                data-volume-id="V0003" data-updated-at="120">A 重复</a></li>
          <li><a class="entry-link" href="/volumes/V0001"
                data-volume-id="V0001" data-updated-at="100">B</a></li>
        </ul>
        <a class="next-page" href="/catalog?page=2">下一页</a>
        </body></html>
        """
        page = parse_catalog(html)
        self.assertEqual([e.volume_id for e in page.entries],
                         ["V0003", "V0003", "V0001"])
        self.assertEqual(page.next_page, "/catalog?page=2")

    def test_parse_detail(self) -> None:
        html = """
        <html><body><article>
        <dd class="field-volume-id">V0006</dd>
        <dd class="field-version">2</dd>
        <dd class="field-updated-at">700</dd>
        <dd class="field-checksum">abc123</dd>
        <dd class="field-size">122880</dd>
        <a class="image-link" href="/images/V0006">x</a>
        </article></body></html>
        """
        detail = parse_detail(html)
        self.assertEqual(detail.volume_id, "V0006")
        self.assertEqual(detail.version, 2)
        self.assertEqual(detail.updated_at, 700)
        self.assertEqual(detail.checksum, "abc123")
        self.assertEqual(detail.image_href, "/images/V0006")


class ScenarioTests(unittest.TestCase):
    def test_backfill_within_ten_minutes(self) -> None:
        from mocksite.scenario import VIRTUAL_EPOCH
        scenario = Scenario()
        v7 = scenario.volumes["V0007"]
        # 更新时间偏移 60，目录偏移 180 才出现：相差 120 站点秒，在 10 分钟窗口内
        self.assertEqual(
            v7.first_seen_at - v7.versions[0].updated_at, 120
        )
        self.assertLess(
            v7.first_seen_at - v7.versions[0].updated_at, 600
        )
        # first_seen_at 之前不可见，之后可见
        self.assertFalse(v7.is_visible(VIRTUAL_EPOCH + 179))
        self.assertTrue(v7.is_visible(VIRTUAL_EPOCH + 180))

    def test_revision_updated_at_increases_and_href_stable(self) -> None:
        vols = build_volumes()
        v6 = vols["V0006"]
        self.assertLess(v6.versions[0].updated_at, v6.versions[1].updated_at)
        self.assertNotEqual(v6.versions[0].checksum, v6.versions[1].checksum)
        # 下载地址沿用由路由 /images/<volume_id> 保证：不带版本号
        self.assertEqual(len(v6.versions), 2)

    def test_deterministic_image_and_checksum(self) -> None:
        s1 = Scenario()
        s2 = Scenario()
        data1 = s1.images[("V0001", 1)]
        data2 = s2.images[("V0001", 1)]
        self.assertEqual(data1, data2)
        import hashlib
        self.assertEqual(
            hashlib.sha256(data1).hexdigest(),
            s1.volumes["V0001"].versions[0].checksum,
        )


class HTTPClientReplayTests(unittest.TestCase):
    """起进程内模拟站，验证客户端的计时口径与暂停语义。"""

    def setUp(self) -> None:
        import threading
        from mocksite.server import ReplayPlan, build_server
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.plan = ReplayPlan()
        self.httpd = build_server("127.0.0.1", 0, rate=20,
                                  replay_plan=self.plan)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.addCleanup(self.httpd.shutdown)
        self.addCleanup(self.httpd.server_close)

    def _client(self, max_retries: int = 3, min_interval: float = 0.05):
        from aerial.http import Client
        from aerial.request_log import RequestLog
        wd = Path(self.tmp.name)
        cfg = Config(
            base_url=f"http://127.0.0.1:{self.port}",
            workdir=wd, min_interval=min_interval,
            max_retries=max_retries, backoff_base=0.01, backoff_cap=0.05,
        )
        limiter = RateLimiter(min_interval)
        req_log = RequestLog(None)  # 只留内存，不落盘
        return Client(cfg, limiter, req_log), limiter, req_log

    def test_pause_applies_even_when_retries_exhausted(self) -> None:
        from aerial.http import TransientHTTPError
        self.plan.set_status("always503", count=5, status=503, retry_after=0.5)
        client, limiter, _ = self._client(max_retries=0)
        with self.assertRaises(TransientHTTPError):
            client.get("/_replay/always503")
        # 请求虽失败，统一暂停必须已登记
        self.assertEqual(limiter.pause_events, 1)
        # 暂停窗口内的新请求被推迟
        t0 = time.monotonic()
        client.get("/_replay/ok")
        self.assertGreaterEqual(time.monotonic() - t0, 0.45)

    def test_429_retry_after_pauses_then_succeeds(self) -> None:
        self.plan.set_status("rate429", count=1, status=429, retry_after=0.3)
        client, limiter, req_log = self._client(max_retries=2)
        resp = client.get("/_replay/rate429")
        self.assertEqual(resp.status, 200)
        self.assertEqual(limiter.pause_events, 1)
        statuses = [r["status"] for r in req_log.records]
        self.assertEqual(statuses[0], 429)
        self.assertIn(200, statuses)
        # 记录里带 Retry-After 与 paused 标记
        self.assertEqual(req_log.records[0]["retry_after"], "0.3")

    def test_min_interval_anchored_at_header_send_time(self) -> None:
        client, _limiter, req_log = self._client(min_interval=0.08)
        for _ in range(4):
            client.get("/_replay/ok")
        sent = [r["t_sent"] for r in req_log.records if r["t_sent"] is not None]
        self.assertEqual(len(sent), 4)
        for a, b in zip(sent, sent[1:]):
            self.assertGreaterEqual(b - a, 0.08 - 0.01)

    def test_slow_body_does_not_block_other_requests(self) -> None:
        import threading
        self.plan.set_slow_body("slow-body", 0.05)
        client, _limiter, req_log = self._client(min_interval=0.05)
        err: list[Exception] = []

        def slow() -> None:
            try:
                client.get("/_replay/slow-body")
            except Exception as exc:  # noqa: BLE001
                err.append(exc)

        t = threading.Thread(target=slow)
        t.start()
        # 等慢请求头发出去
        deadline = time.monotonic() + 3
        while not req_log.records and time.monotonic() < deadline:
            time.sleep(0.005)
        t0 = time.monotonic()
        client.get("/_replay/ok")
        peer_gap = time.monotonic() - t0
        t.join(timeout=10)
        self.assertFalse(err)
        # 同伴请求只受最小间隔约束，没有被慢响应体拖住
        self.assertLess(peer_gap, 0.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
