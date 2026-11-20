"""全量 / 增量采集调度。

扫描流程：
1. 从 /catalog 开始逐页解析目录（目录按更新时间倒序、可能有重复链接）；
2. 全量模式：每个卷宗取一个链接进详情；
   增量模式：只进“上次扫描水位之后的更新”与“仍可能补登的范围”
   （上次扫描水位 - backfill_seconds 地平线以内）的卷宗。目录按更新时间
   倒序，因此一页里最早的时间都低于地平线时即可停止翻页——大目录里每天
   只变几卷，增量不会翻完整个目录；
3. 已知卷宗按“目录条目的 updated_at > 本地已知版本 updated_at”判定换版，
   未变卷不拉详情；
4. worker 线程池并发处理详情与下载，全部请求共用一个限流器；
5. 临时失败进持久化 FailedQueue，主扫描结束后跑补齐轮次（从目录首页复查
   到地平线为止），兜住“最迟在更新时间后 backfill_seconds 补登”的条目与
   翻页期间插入的条目；
6. 每翻完一页就把“下一页地址 + 本轮已接受候选”原子落盘；进程被 kill -9
   后用同一工作目录重跑，从断点继续，不重读已确认的目录页。
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .config import Config
from .downloader import ImageDownloader
from .http import Client, PermanentHTTPError, TransientHTTPError
from .limiter import RateLimiter
from .manifest import Manifest
from .parsing import CatalogEntry, CatalogPage, VolumeDetail, parse_catalog, parse_detail
from .state import CatalogProgress, FailedItem, FailedQueue, ScanState, WorkDir

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
        # 每个卷宗本地已知的最新版本更新时间：换版判定的唯一依据。
        # 来源依次为：本次运行抓到的详情、state.json、manifest/current.json。
        self._known_updated: dict[str, int] = dict(self.state.known_updated_at)
        for vid, entry in self.manifest.snapshot().items():
            updated = entry.get("updated_at")
            if isinstance(updated, int):
                prev = self._known_updated.get(vid)
                self._known_updated[vid] = updated if prev is None else max(prev, updated)

        self.max_site_time: Optional[int] = self.state.last_scan_site_time
        # 本次扫描启动那一刻的站点时间（用作持久化水位）
        self._scan_start_site_time: Optional[int] = None
        self.stop_event = threading.Event()
        # 主扫描是否遇到过空目录（条目可能尚未补登，需继续复查）
        self._catalog_was_empty = False
        self._empty_seen_site_time: Optional[int] = None
        # 最近一次目录复查时的站点时间，用于限定补登等待上限
        self._last_recheck_site_time: Optional[int] = None
        # 增量补登地平线（站点秒）：updated_at 低于它的条目不再复查
        self._floor: Optional[int] = None
        # 目录翻页断点（强杀续采）
        self.progress: Optional[CatalogProgress] = None
        self._candidates_file: Path = workdir.state_dir / "scan.candidates.jsonl"

    # ------------------------------------------------------------------ #
    def run(self) -> int:
        log.info("==== 开始 %s 采集，工作目录 %s ====", self.mode, self.cfg.workdir)
        self._begin_or_resume()

        # 上次未补齐的卷宗先入队
        for item in self.failed.all():
            self._enqueue(Job(item.volume_id, item.href or f"/volumes/{item.volume_id}",
                              item.updated_at, item.attempts))
        # 断点里已确认的候选重新入队（已完成/已被更新版取代的会按规则跳过）
        self._restore_candidates()

        self._scan_catalog()

        # 主扫描 worker 池（守护线程，队列 join 后随进程退出）
        self._start_workers()
        self.jobs.join()

        # 补齐轮次：失败重试 + 补登复查。
        # 继续条件（满足其一）：
        #   a) 失败队列非空（有卷宗要重试）；
        #   b) 主扫描遇到过空目录，或最近一次复查时站点时间仍落在
        #      “补登地平线 + 一个窗口”之内——迟到条目最迟在更新时间后
        #      backfill_seconds 内补登，必须持续复查到窗口走完。
        self._recheck_rounds()

        # 保存扫描水位：用“扫描启动时刻”而非结束时刻，保证扫描期间生效、
        # 但本次未采到的更新，下一次增量仍会被复查（不会被水位跳过）。
        # 水位只在整轮扫描成功结束后推进；中途强杀走不到这里，
        # 断点续采沿用旧水位，不会把未完成的扫描当成已完成。
        if self._scan_start_site_time is not None:
            self.state.last_scan_site_time = self._scan_start_site_time
        self.state.mode = self.mode
        self.state.known_updated_at = dict(self._known_updated)
        self.state.save(self.wd.state_file)
        self._clear_progress()

        remaining = len(self.failed)
        log.info("==== %s 采集结束：清单 %d 卷，待补齐 %d 卷，限流统计 %s ====",
                 self.mode, self.manifest.volume_count(), remaining,
                 self.limiter.snapshot())
        return 0 if remaining == 0 else 2

    # ------------------------------------------------------------------ #
    # 断点 / 地平线
    # ------------------------------------------------------------------ #
    def _begin_or_resume(self) -> None:
        """加载或新建目录翻页断点。

        只有模式与“上一轮水位”都对得上时才续采：说明这是同一次扫描被
        kill 后的重跑，已确认的目录页可以跳过；否则按一轮全新扫描处理。
        """
        progress = CatalogProgress.load(self.wd.scan_file)
        if progress is not None and progress.matches(
                self.mode, self.state.last_scan_site_time):
            self.progress = progress
            self._scan_start_site_time = progress.round_start_site_time
            self._floor = progress.floor
            self._catalog_was_empty = progress.catalog_was_empty
            self._empty_seen_site_time = progress.empty_seen_site_time
            self._last_recheck_site_time = progress.last_recheck_site_time
            log.info(
                "沿用上次未完成的目录断点：主扫描%s（已确认 %d 页，下一页 %s），"
                "补齐轮 %d/%d，水位 %s，地平线 %s",
                "已完成" if progress.main_done else "进行中",
                progress.main_pages_done, progress.main_next_url,
                progress.recheck_done_rounds, self.cfg.retry_rounds,
                self._scan_start_site_time, self._floor,
            )
            return

        # 全新一轮：清掉可能留下的旧断点文件
        self._discard_candidates()
        self.wd.scan_file.unlink(missing_ok=True)
        self.progress = CatalogProgress(
            mode=self.mode,
            prev_watermark=self.state.last_scan_site_time,
        )
        if self.mode == "incremental":
            self._floor = self._incremental_floor_from_state()

    def _incremental_floor_from_state(self) -> Optional[int]:
        """增量地平线：上次扫描水位 - 补登窗口。

        不看“现在”：停采再久，也只需回看到上次扫描时仍可能迟到补登的
        范围（水位 - 窗口）。低于它的条目在上次扫描时必定已经出现并处理
        过，不必再翻。
        """
        base = self.state.last_scan_site_time
        if base is None:
            return None
        return base - self.cfg.backfill_seconds

    def _recheck_floor(self) -> Optional[int]:
        """补齐轮次复查的下界：扫描启动时刻 - 补登窗口。

        主扫描启动时不可见的条目，其更新时间必然晚于“启动时刻 - 窗口”
        （条目最迟在更新时间后一个窗口内上目录）。因此复查只需从首页翻到
        这个下界，就能捞到翻页期间插入 / 迟到补登的条目，不必每轮重翻整个
        目录。上一轮的补登地平线更高时取两者较大值。
        """
        if self._scan_start_site_time is None:
            return self._floor
        floor = self._scan_start_site_time - self.cfg.backfill_seconds
        if self._floor is not None:
            floor = max(floor, self._floor)
        return floor

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
        # 只有校验通过并提交后才推进本地版本基线：半路失败（含 kill -9）
        # 的版本下次增量必须重新查详情，不能被基线永久跳过。
        prev = self._known_updated.get(vid)
        if prev is None or detail.updated_at > prev:
            self._known_updated[vid] = detail.updated_at

    def _fetch_detail(self, job: Job) -> VolumeDetail:
        resp = self.client.get(job.href)
        self._note_site_time(resp.site_time)
        return parse_detail(resp.data.decode("utf-8"))

    # ------------------------------------------------------------------ #
    # 主扫描
    # ------------------------------------------------------------------ #
    def _scan_catalog(self) -> None:
        """翻目录并把候选卷宗入队；可从上次确认的下一页续采。"""
        if self.progress.main_done:
            log.info("目录主扫描上次已完成（已确认 %d 页），本次不重读",
                     self.progress.main_pages_done)
            return

        url: Optional[str] = self.progress.main_next_url or "/catalog"
        page_no = self.progress.main_pages_done
        while url is not None and not self.stop_event.is_set():
            page_no += 1
            resp = self.client.get(url)
            self._note_site_time(resp.site_time)
            self._ensure_floor(resp.site_time)
            page = parse_catalog(resp.data.decode("utf-8"))

            # 空目录：条目可能还没补登，标记后交给补齐轮次持续复查
            if not page.entries:
                self._catalog_was_empty = True
                self._empty_seen_site_time = resp.site_time
                log.info("第 %d 页目录为空，条目可能尚未补登，稍后复查", page_no)

            for entry in page.entries:
                self._consider_entry(entry, self._floor)

            url = page.next_page
            stop = self._should_stop_paging(page, self._floor)
            self.progress.main_pages_done = page_no
            self.progress.main_next_url = None if stop else url
            self.progress.main_done = stop or url is None
            self._save_progress()
            if stop:
                log.info("第 %d 页条目均早于补登地平线 %s，增量提前结束翻页",
                         page_no, self._floor)
                break

        if self.progress.main_next_url is None:
            self.progress.main_done = True
        self._save_progress()
        log.info("目录扫描完成：共 %d 页（含上次断点），去重后候选 %d 个",
                 page_no, len(self._seen_catalog))

    def _should_stop_paging(self, page: CatalogPage,
                            floor: Optional[int]) -> bool:
        """目录按更新时间倒序：整页都早于地平线时后续页只会更旧，可停止。

        全量模式（floor=None）必须翻完整个目录。
        """
        if floor is None or not page.entries:
            return False
        return max(e.updated_at for e in page.entries) < floor

    def _ensure_floor(self, site_time: Optional[int]) -> None:
        """增量第一页拿到响应头站点时间后确定地平线并落盘。

        有上次水位：上次水位 - 窗口（停采再久也只看这一范围）；
        没有上次水位（首次增量）：当前站点时间 - 窗口。
        """
        if self.mode != "incremental" or self._floor is not None:
            return
        floor = self._incremental_floor_from_state()
        if floor is None and site_time is not None:
            floor = site_time - self.cfg.backfill_seconds
        self._floor = floor
        self.progress.floor = floor

    # ------------------------------------------------------------------ #
    # 补齐轮次
    # ------------------------------------------------------------------ #
    def _recheck_rounds(self) -> None:
        for round_no in range(1, self.cfg.retry_rounds + 1):
            if self.stop_event.is_set():
                break
            if round_no <= self.progress.recheck_done_rounds:
                continue
            # 上一轮复查中途失败/被强杀：断点里有下一页，直接从该页继续
            resuming = (self.progress.recheck_next_url is not None
                        and self.progress.recheck_pages_done > 0)
            if resuming:
                self.progress.recheck_round = round_no
                log.info("补齐轮次 %d：从断点续查（已确认 %d 页）",
                         round_no, self.progress.recheck_pages_done)
            else:
                time.sleep(self.cfg.retry_round_interval)
                self.progress.recheck_round = round_no
                self.progress.recheck_next_url = "/catalog"
                self.progress.recheck_pages_done = 0
                self._save_progress()

            error = None
            try:
                found_new = self._recheck_catalog()
            except TransientHTTPError as exc:
                # 断点保留在当前页，下一轮从该页继续
                log.warning("补登复查失败，下轮再试: %s", exc)
                error = exc
                found_new = False
            else:
                self.progress.recheck_done_rounds = round_no
                self.progress.recheck_round = 0
                self.progress.recheck_next_url = None
                self.progress.recheck_pages_done = 0
                self._save_progress()

            pending = self.failed.all()
            if pending:
                log.info("补齐轮次 %d：%d 个卷宗待补齐 %s",
                         round_no, len(pending), [x.volume_id for x in pending])
                for item in pending:
                    self._enqueue(Job(item.volume_id,
                                      item.href or f"/volumes/{item.volume_id}",
                                      item.updated_at, item.attempts))
            if pending or found_new:
                # 本轮复查发现的候选先处理完，再判断能否结束
                self.jobs.join()
            if error is not None:
                continue
            if self.failed.all():
                continue  # 补齐中再次失败，下一轮重试
            if not self._should_keep_waiting():
                log.info("补齐轮次 %d：无待办且补登窗口已走完，结束", round_no)
                break
            log.info("补齐轮次 %d：暂无待办，继续等待补登条目", round_no)

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

    def _recheck_catalog(self) -> bool:
        """补齐轮次中从断点页继续翻目录，捞取晚补登 / 换版 / 翻页期间插入。

        与主扫描一样按地平线提前停止；返回是否发现新候选。
        """
        before = len({k[0] for k in self._seen_catalog})
        floor = self._recheck_floor()
        url: Optional[str] = self.progress.recheck_next_url or "/catalog"
        page_no = self.progress.recheck_pages_done
        if self.progress.recheck_next_url is None:
            page_no = 0
        while url is not None:
            page_no += 1
            resp = self.client.get(url)
            self._note_site_time(resp.site_time)
            self._last_recheck_site_time = resp.site_time
            page = parse_catalog(resp.data.decode("utf-8"))
            for entry in page.entries:
                self._consider_entry(entry, floor)
            url = page.next_page
            stop = self._should_stop_paging(page, floor)
            self.progress.recheck_pages_done = page_no
            self.progress.recheck_next_url = None if stop else url
            self._save_progress()
            if stop:
                break
        found = len({k[0] for k in self._seen_catalog}) > before
        if found:
            log.info("补登复查（%d 页）发现新候选卷宗", page_no)
        return found

    # ------------------------------------------------------------------ #
    # 候选判定
    # ------------------------------------------------------------------ #
    def _consider_entry(self, entry: CatalogEntry, floor: Optional[int]) -> bool:
        """判定目录条目是否值得拉详情。

        - 全量：全部接受；
        - 增量已知卷宗：目录 updated_at 严格大于本地已采版本才算换版
          （相等即未变，不查详情；水位同秒发生的换版因 updated_at 更大
          仍然会被接受）；
        - 增量未知卷宗：updated_at >= 地平线才可能是迟到补登。
        """
        if not self._should_fetch(entry, floor):
            return False
        dedup_key = (entry.volume_id, entry.updated_at)
        if dedup_key in self._seen_catalog:
            log.info("目录重复链接已忽略: %s updated_at=%s（%s）",
                     entry.volume_id, entry.updated_at, entry.href)
            return False
        self._seen_catalog.add(dedup_key)
        self._append_candidate(entry)
        self._enqueue(Job(entry.volume_id, entry.href, entry.updated_at))
        return True

    def _should_fetch(self, entry: CatalogEntry, floor: Optional[int]) -> bool:
        if self.mode == "full":
            return True
        known_updated = self._known_updated.get(entry.volume_id)
        if known_updated is None:
            cur = self.manifest.current_entry(entry.volume_id)
            if cur is not None:
                known_updated = cur.get("updated_at")
        if known_updated is not None:
            # 目录 updated_at 严格大于本地已采版本才算换版；相等即未变
            return entry.updated_at > known_updated
        if entry.volume_id in self.state.known_volumes:
            # 已知但缺基线（旧版状态文件）：退化为与上次扫描水位比较，
            # 只有严格更新的才查详情
            watermark = self.state.last_scan_site_time
            return watermark is None or entry.updated_at > watermark
        if floor is None:
            return True
        return entry.updated_at >= floor

    # ------------------------------------------------------------------ #
    # 断点文件
    # ------------------------------------------------------------------ #
    def _save_progress(self) -> None:
        p = self.progress
        p.round_start_site_time = self._scan_start_site_time
        p.floor = self._floor
        p.catalog_was_empty = self._catalog_was_empty
        p.empty_seen_site_time = self._empty_seen_site_time
        p.last_recheck_site_time = self._last_recheck_site_time
        p.save(self.wd.scan_file)

    def _discard_candidates(self) -> None:
        """开始新一轮之前清空候选文件（断点确认可续采时不动）。"""
        self._candidates_file.unlink(missing_ok=True)

    def _append_candidate(self, entry: CatalogEntry) -> None:
        """把已接受的候选追加到断点候选文件（每页 fsync 一次）。"""
        line = f"{entry.volume_id}\t{entry.href}\t{entry.updated_at}\n"
        with open(self._candidates_file, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())

    def _load_candidates(self) -> list[CatalogEntry]:
        if not self._candidates_file.exists():
            return []
        entries: list[CatalogEntry] = []
        for raw in self._candidates_file.read_text(encoding="utf-8").splitlines():
            parts = raw.split("\t")
            if len(parts) != 3:
                continue
            vid, href, updated = parts
            try:
                entries.append(CatalogEntry(vid, href, int(updated)))
            except ValueError:
                continue
        return entries

    def _restore_candidates(self) -> None:
        restored = 0
        for entry in self._load_candidates():
            key = (entry.volume_id, entry.updated_at)
            if key in self._seen_catalog:
                continue
            self._seen_catalog.add(key)
            if not self._should_fetch(entry, self._floor):
                continue
            cur = self.manifest.current_entry(entry.volume_id)
            if cur is not None and cur.get("updated_at") == entry.updated_at:
                # 断点前已完成该版本：不再重复拉详情
                continue
            self._enqueue(Job(entry.volume_id, entry.href, entry.updated_at))
            restored += 1
        if restored:
            log.info("断点候选已恢复 %d 个（已完成 / 已被新版取代的已跳过）", restored)

    def _clear_progress(self) -> None:
        self.wd.scan_file.unlink(missing_ok=True)
        self._candidates_file.unlink(missing_ok=True)

    # ------------------------------------------------------------------ #
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
