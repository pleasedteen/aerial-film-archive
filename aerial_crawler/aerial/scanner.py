"""全量 / 增量采集调度。

扫描流程：
1. 从 /catalog 开始逐页解析目录（目录按更新时间倒序、可能有重复链接）；
2. 全量模式：每个卷宗取一个链接进详情；
   增量模式：只进“上次扫描水位之后的更新”与“仍可能补登的范围”
   （站点当前时间 - backfill_seconds 地平线以内）的卷宗，已知卷宗整体落在
   地平线之外的页面可提前停止翻页；
3. worker 线程池并发处理详情与下载，全部请求共用一个限流器；
4. 临时失败进持久化 FailedQueue，主扫描结束后跑补齐轮次（含目录首页复查，
   专门兜住“最迟在更新时间后 10 分钟补登”的条目）；
5. 每轮结束保存扫描水位；强制 kill 后用同一工作目录重跑即可续采。
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Optional

from .config import Config
from .downloader import ImageDownloader
from .http import Client, PermanentHTTPError, TransientHTTPError
from .limiter import RateLimiter
from .manifest import Manifest
from .parsing import CatalogEntry, VolumeDetail, parse_catalog, parse_detail
from .state import FailedItem, FailedQueue, ScanState, WorkDir

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 8  # 单个卷宗跨进程的最大尝试次数，超过则留在失败队列人工处理


@dataclass
class Job:
    """一个待处理卷宗（目录链接信息；详情可能尚未拉取）。"""
    volume_id: str
    href: str
    updated_at: Optional[int]
    attempts: int = 0


class Scanner:
    def __init__(self, cfg: Config, workdir: WorkDir, mode: str) -> None:
        if mode not in ("full", "incremental"):
            raise ValueError("mode 只能是 full / incremental")
        self.cfg = cfg
        self.wd = workdir
        self.mode = mode
        self.limiter = RateLimiter(cfg.min_interval)
        self.client = Client(cfg, self.limiter)
        self.manifest = Manifest(workdir.events_file, workdir.current_file)
        self.downloader = ImageDownloader(cfg, self.client, self.manifest)
        self.state = ScanState.load(workdir.state_file)
        self.failed = FailedQueue(workdir.failed_file)

        self.jobs: "queue.Queue[Optional[Job]]" = queue.Queue()
        self._inflight: set[str] = set()
        self._inflight_lock = threading.Lock()
        # 去重键为 (卷宗号, 更新时间)：同版重复链接去重，换版仍会重新入队
        self._seen_catalog: set[tuple[str, int]] = set()
        self.max_site_time: Optional[int] = self.state.last_scan_site_time
        # 本次扫描启动那一刻的站点时间（用作持久化水位）
        self._scan_start_site_time: Optional[int] = None
        self.stop_event = threading.Event()
        # 主扫描是否遇到过空目录（条目可能尚未补登，需继续复查）
        self._catalog_was_empty = False
        self._empty_seen_site_time: Optional[int] = None
        # 最近一次目录复查时的站点时间，用于限定补登等待上限
        self._last_recheck_site_time: Optional[int] = None

    # ------------------------------------------------------------------ #
    def run(self) -> int:
        log.info("==== 开始 %s 采集，工作目录 %s ====", self.mode, self.cfg.workdir)
        # 上次未补齐的卷宗先入队
        for item in self.failed.all():
            self._enqueue(Job(item.volume_id, item.href or f"/volumes/{item.volume_id}",
                              item.updated_at, item.attempts))

        horizon = self._scan_catalog()

        # 主扫描 worker 池（守护线程，队列 join 后随进程退出）
        self._start_workers()
        self.jobs.join()

        # 补齐轮次：失败重试 + 补登复查。
        # 继续条件（满足其一）：
        #   a) 失败队列非空（有卷宗要重试）；
        #   b) 主扫描遇到过空目录，或最近一次复查时站点时间仍落在
        #      “补登地平线 + 一个窗口”之内——迟到条目最迟在更新时间后
        #      backfill_seconds 内补登，必须持续复查到窗口走完。
        for round_no in range(1, self.cfg.retry_rounds + 1):
            if self.stop_event.is_set():
                break
            time.sleep(self.cfg.retry_round_interval)
            found_new = self._recheck_catalog(horizon)
            pending = self.failed.all()
            if not pending and not self._should_keep_waiting():
                log.info("补齐轮次 %d：无待办且补登窗口已走完，结束", round_no)
                break
            if not pending and not found_new:
                log.info("补齐轮次 %d：暂无待办，继续等待补登条目", round_no)
            if pending:
                log.info("补齐轮次 %d：%d 个卷宗待补齐 %s",
                         round_no, len(pending), [x.volume_id for x in pending])
                for item in pending:
                    self._enqueue(Job(item.volume_id,
                                      item.href or f"/volumes/{item.volume_id}",
                                      item.updated_at, item.attempts))
                self.jobs.join()

        # 保存扫描水位：用“扫描启动时刻”而非结束时刻，保证扫描期间生效、
        # 但本次未采到的更新，下一次增量仍会被复查（不会被水位跳过）。
        if self._scan_start_site_time is not None:
            self.state.last_scan_site_time = self._scan_start_site_time
        self.state.mode = self.mode
        self.state.save(self.wd.state_file)

        remaining = len(self.failed)
        log.info("==== %s 采集结束：清单 %d 卷，待补齐 %d 卷，限流统计 %s ====",
                 self.mode, self.manifest.volume_count(), remaining,
                 self.limiter.snapshot())
        return 0 if remaining == 0 else 2

    # ------------------------------------------------------------------ #
    def _start_workers(self) -> None:
        for i in range(self.cfg.workers):
            t = threading.Thread(target=self._worker_loop, name=f"worker-{i}",
                                 daemon=True)
            t.start()

    def _worker_loop(self) -> None:
        while True:
            job = self.jobs.get()
            try:
                self._process_job(job)
            except Exception:  # noqa: BLE001 - worker 不能因单卷异常整体退出
                log.exception("处理卷宗 %s 时出现未预期异常", job.volume_id)
                self._mark_failed(job, "未预期异常")
            finally:
                with self._inflight_lock:
                    self._inflight.discard(job.volume_id)
                self.jobs.task_done()

    def _process_job(self, job: Job) -> None:
        vid = job.volume_id
        try:
            detail = self._fetch_detail(job)
        except PermanentHTTPError as exc:
            if exc.status == 404:
                # 目录已出现但详情还没开放：补登窗口内按临时失败处理
                self._mark_failed(job, f"详情 404，等待补登（{job.href}）")
                return
            self._mark_failed(job, f"详情永久错误 {exc.status}")
            return
        except TransientHTTPError as exc:
            self._mark_failed(job, f"详情临时故障: {exc}")
            return

        self.state.known_volumes.add(vid)
        self.manifest.record_discovered(detail)

        try:
            self.downloader.download(detail)
        except TransientHTTPError as exc:
            self._mark_failed(job, f"影像下载临时故障: {exc}", detail=detail)
            self.manifest.record_failure(vid, detail, str(exc))
            return
        except PermanentHTTPError as exc:
            self._mark_failed(job, f"影像永久错误 {exc.status}", detail=detail)
            return

        self.failed.remove(vid)

    def _fetch_detail(self, job: Job) -> VolumeDetail:
        resp = self.client.get(job.href)
        self._note_site_time(resp.site_time)
        return parse_detail(resp.data.decode("utf-8"))

    # ------------------------------------------------------------------ #
    def _scan_catalog(self) -> Optional[int]:
        """翻完目录并把候选卷宗入队，返回本轮使用的补登地平线（可能为 None）。"""
        url: Optional[str] = "/catalog"
        page_no = 0
        horizon: Optional[int] = None
        while url is not None and not self.stop_event.is_set():
            page_no += 1
            resp = self.client.get(url)
            self._note_site_time(resp.site_time)
            page = parse_catalog(resp.data.decode("utf-8"))

            if self.mode == "incremental":
                horizon = self._current_horizon(resp.site_time)

            # 空目录：条目可能还没补登，标记后交给补齐轮次持续复查
            if not page.entries:
                self._catalog_was_empty = True
                self._empty_seen_site_time = resp.site_time
                log.info("第 %d 页目录为空，条目可能尚未补登，稍后复查", page_no)

            # 注：不在此处按“整页均为旧卷宗”提前停止翻页。目录按更新时间倒序，
            # 但换版条目会插到前部页面、补登条目位置不定，提前停止会漏掉它们；
            # 增量的范围裁剪统一由 _consider_entry 的水位 / 地平线规则负责。
            for entry in page.entries:
                self._consider_entry(entry, horizon)
            url = page.next_page
        log.info("目录扫描完成：共 %d 页，去重后候选 %d 个", page_no,
                 len({k[0] for k in self._seen_catalog}))
        return horizon

    def _should_keep_waiting(self) -> bool:
        """是否还要继续复查目录等待补登。

        依据站点时钟与补登窗口判定：只要“当前站点时间 - 补登窗口”仍晚于
        首次观察到空目录的时刻，就说明该时刻之后更新的条目仍处于可能补登
        的范围内，必须继续复查。窗口走完且失败队列为空才可停止。
        未观察到空目录的常规扫描不等待。
        """
        if not self._catalog_was_empty:
            return False
        if self._empty_seen_site_time is None or \
                self._last_recheck_site_time is None:
            return True
        horizon_now = self._last_recheck_site_time - self.cfg.backfill_seconds
        return horizon_now <= self._empty_seen_site_time

    def _recheck_catalog(self, horizon: Optional[int]) -> bool:
        """补齐轮次中完整重翻目录，捞取晚补登 / 换版的条目。

        不能只看首页：补登条目可能出现在后续分页。返回是否发现新候选。
        """
        before = len({k[0] for k in self._seen_catalog})
        url: Optional[str] = "/catalog"
        page_no = 0
        try:
            while url is not None:
                page_no += 1
                resp = self.client.get(url)
                self._note_site_time(resp.site_time)
                self._last_recheck_site_time = resp.site_time
                if horizon is None and self.mode == "incremental":
                    horizon = self._current_horizon(resp.site_time)
                page = parse_catalog(resp.data.decode("utf-8"))
                for entry in page.entries:
                    self._consider_entry(entry, horizon)
                url = page.next_page
        except TransientHTTPError as exc:
            log.warning("补登复查失败，下轮再试: %s", exc)
            return False
        found = len({k[0] for k in self._seen_catalog}) > before
        if found:
            log.info("补登复查（%d 页）发现新候选卷宗", page_no)
        return found

    def _consider_entry(self, entry: CatalogEntry, horizon: Optional[int]) -> None:
        vid = entry.volume_id
        if self.mode == "incremental":
            known = vid in self.state.known_volumes
            # 已知卷宗只看水位之后的更新；未知卷宗在地平线内才值得查
            # （地平线之外的未知条目说明其更新时间太旧，不会再补登）
            if known:
                if self.state.last_scan_site_time is not None and \
                        entry.updated_at <= self.state.last_scan_site_time:
                    return
            elif horizon is not None and entry.updated_at < horizon:
                return
        # 去重键含更新时间：同一卷同一版本的重复链接只入队一次，
        # 但复查时发现的换版（updated_at 变大）会作为新任务入队
        dedup_key = (vid, entry.updated_at)
        if dedup_key in self._seen_catalog:
            log.info("目录重复链接已忽略: %s updated_at=%s（%s）",
                     vid, entry.updated_at, entry.href)
            return
        self._seen_catalog.add(dedup_key)
        self._enqueue(Job(vid, entry.href, entry.updated_at))

    def _current_horizon(self, site_time: Optional[int]) -> Optional[int]:
        if site_time is None:
            return None
        return site_time - self.cfg.backfill_seconds

    def _note_site_time(self, site_time: Optional[int]) -> None:
        if site_time is None:
            return
        if self._scan_start_site_time is None:
            self._scan_start_site_time = site_time
        if self.max_site_time is None or site_time > self.max_site_time:
            self.max_site_time = site_time

    # ------------------------------------------------------------------ #
    def _enqueue(self, job: Job) -> None:
        with self._inflight_lock:
            if job.volume_id in self._inflight:
                return
            self._inflight.add(job.volume_id)
        self.jobs.put(job)

    def _mark_failed(self, job: Job, reason: str,
                     detail: Optional[VolumeDetail] = None) -> None:
        attempts = job.attempts + 1
        if attempts > MAX_ATTEMPTS:
            log.error("卷宗 %s 尝试 %d 次仍失败，保留在失败队列: %s",
                      job.volume_id, attempts, reason)
        else:
            log.warning("卷宗 %s 标记待补齐（第 %d 次）: %s",
                        job.volume_id, attempts, reason)
        self.failed.upsert(FailedItem(
            volume_id=job.volume_id,
            href=job.href,
            updated_at=detail.updated_at if detail else job.updated_at,
            attempts=attempts,
        ))
