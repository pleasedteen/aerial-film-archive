"""采集会话：一个会话 = 一次全量或增量采集。

设计目标
--------
- **同一进程内可并行**：每个会话占用一个工作目录，互不影响；同一站点上的
  多个会话共享该站点的最小请求间隔与统一暂停（站点口径），但各自有独立的
  取消令牌，某个会话停止不影响同站点其他会话。
- **可独立停止**：``Session.stop()`` 在 2 秒内完成——先取消令牌（唤醒所有
  在途请求，包括正在等目录响应 / 站点暂停 / 影像响应体的），再合上写入门闩
  并等待在途写入排空，最后交还工作目录锁。返回后该会话不能再发请求、不能再
  写文件（即便有残留线程，写门闩也会拒绝它）。
- **可续采**：停止时保留已 fsync 的区段与已提交的清单；用原工作目录重新
  启动会复用区段、只补缺档，清单里始终只有完整且校验通过的影像。
- **站点隔离**：不同站点的会话使用各自独立的限流器，停止一个站的会话不会
  波及另一个站的采集。

``SessionManager`` 负责按站点缓存共享限流器，是并行跑多个会话的入口。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import replace
from typing import Optional

from .cancel import CancelToken
from .config import Config
from .http import Client
from .limiter import RateLimiter
from .manifest import Manifest
from .downloader import ImageDownloader
from .scanner import EXIT_CANCELLED, Scanner
from .state import WorkDir

log = logging.getLogger(__name__)

STOP_TIMEOUT = 2.0        # 停采必须在 2 秒内交还工作目录
_WRITE_DRAIN_RATIO = 0.6  # 停采预算中留给“等在途写入排空”的比例


class SessionConflict(RuntimeError):
    """工作目录已被另一个会话占用。"""


class Session:
    """一次可独立启动 / 停止的采集会话。"""

    def __init__(self, cfg: Config, mode: str, limiter: RateLimiter,
                 name: Optional[str] = None) -> None:
        if mode not in ("full", "incremental"):
            raise ValueError("mode 只能是 full / incremental")
        self.cfg = cfg
        self.mode = mode
        self.name = name or f"{mode}:{cfg.workdir}"
        self.limiter = limiter
        self.token = CancelToken()
        self.wd = WorkDir(cfg.workdir)
        self._thread: Optional[threading.Thread] = None
        self._done = threading.Event()
        self._result: Optional[int] = None
        self._scanner: Optional[Scanner] = None
        self._lock_held = False
        self._state_lock = threading.Lock()

    # ------------------------------------------------------------------ #
    @property
    def result(self) -> Optional[int]:
        return self._result

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def finished(self) -> bool:
        return self._done.is_set()

    # ------------------------------------------------------------------ #
    def start(self) -> "Session":
        """占用工作目录并启动后台线程；目录被占用时抛 SessionConflict。"""
        with self._state_lock:
            if self._thread is not None:
                raise RuntimeError("会话只能启动一次")
            try:
                self.wd.acquire_lock()
                self._lock_held = True
            except RuntimeError as exc:
                raise SessionConflict(str(exc)) from exc
            # 限流器可能属于一个比本次配置更保守的站点上下文，沿用共享实例
            client = Client(self.cfg, self.limiter, self.token)
            manifest = Manifest(self.wd.events_file, self.wd.current_file,
                                self.token)
            downloader = ImageDownloader(self.cfg, client, manifest, self.token)
            self._scanner = Scanner(
                self.cfg, self.wd, self.mode,
                token=self.token, limiter=self.limiter, client=client,
                manifest=manifest, downloader=downloader,
            )
            self._thread = threading.Thread(
                target=self._run, name=f"session[{self.name}]", daemon=True)
            self._thread.start()
        return self

    def _run(self) -> None:
        try:
            result = self._scanner.run() if self._scanner else EXIT_CANCELLED
        except BaseException:  # noqa: BLE001 - 线程异常也要释放锁
            log.exception("会话 %s 异常退出", self.name)
            result = 1
        finally:
            self._release_lock()
            self._done.set()
        self._result = result
        log.info("会话 %s 结束，exit=%s", self.name, result)

    # ------------------------------------------------------------------ #
    def wait(self, timeout: Optional[float] = None) -> Optional[int]:
        """等待会话自然结束；超时返回 None（会话仍在运行）。"""
        finished = self._done.wait(timeout)
        if not finished:
            return None
        return self._result

    def join(self) -> int:
        """阻塞等待会话自然结束（不主动停止）。"""
        self._done.wait()
        return self._result if self._result is not None else EXIT_CANCELLED

    # ------------------------------------------------------------------ #
    def stop(self, timeout: float = STOP_TIMEOUT) -> Optional[int]:
        """停止会话并在 timeout 内交还工作目录。

        顺序：取消令牌 → 叫醒限流等待 → 合上写门闩并等待在途写入 → 交还
        工作目录锁 → 限时等待线程退出。返回退出码（会话已自然结束时返回其
        真实退出码）。
        """
        deadline = time.monotonic() + max(0.0, timeout)
        with self._state_lock:
            already_done = self._done.is_set()
        if not already_done:
            self.token.cancel()
            self.limiter.wake()
            remaining = deadline - time.monotonic()
            drain_budget = max(0.0, remaining * _WRITE_DRAIN_RATIO)
            if not self.token.close_writes(drain_budget):
                log.warning("会话 %s 仍有 %d 个在途写入未排空，写门闩已关闭",
                            self.name, self.token.active_writes)
            # 无论线程是否退出，都必须在此交还工作目录
            self._release_lock()

        # 限时等待线程退出；不阻塞超过预算
        thread = self._thread
        if thread is not None:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                thread.join(timeout=remaining)

        if self._done.is_set():
            return self._result
        return EXIT_CANCELLED

    def _release_lock(self) -> None:
        with self._state_lock:
            if self._lock_held:
                try:
                    self.wd.release_lock()
                finally:
                    self._lock_held = False

    # ------------------------------------------------------------------ #
    def __enter__(self) -> "Session":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.token.cancelled:
            return
        self.stop()


class SessionManager:
    """按站点共享限流器的会话工厂，便于同进程并行跑多个会话。"""

    def __init__(self) -> None:
        self._limiters: dict[str, RateLimiter] = {}
        self._lock = threading.Lock()

    def limiter_for(self, base_url: str, min_interval: float) -> RateLimiter:
        with self._lock:
            limiter = self._limiters.get(base_url)
            if limiter is None:
                limiter = RateLimiter(min_interval)
                self._limiters[base_url] = limiter
            else:
                # 后加入的会话更保守时收紧站点口径
                limiter.min_interval = min(limiter.min_interval, min_interval)
            return limiter

    def create(self, cfg: Config, mode: str,
               name: Optional[str] = None) -> Session:
        limiter = self.limiter_for(cfg.base_url, cfg.min_interval)
        return Session(cfg, mode, limiter, name=name)

    def station_limiter(self, base_url: str) -> Optional[RateLimiter]:
        with self._lock:
            return self._limiters.get(base_url)
