"""全站共享的请求限流器。

约束：
1. 所有发往站点的请求（目录页、详情、影像区段）在发起前都必须先 acquire()，
   保证任意两次请求发起之间至少相隔 min_interval 秒——无论多少个 worker 并发、
   多少个会话并行。
2. 站点返回 503/429 并带 Retry-After 时，调用 pause() 统一推迟“所有”新请求，
   而不是只让命中的那一个 worker 睡眠。

会话独立性：
- 同一站点的多个会话共享同一个限流器实例（站点口径的最小间隔与统一暂停），
  但每次 acquire 都绑定调用方自己的取消令牌；
- 某个会话被停止时只让它自己的等待抛 Cancelled，另一会话继续运行。

依赖：``RateLimiter`` 不 import ``cancel``，只依赖“有 ``check()`` 方法”的
令牌，避免循环导入。
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

log = logging.getLogger(__name__)

# 取消令牌的检查粒度：阻塞等待最多每隔这么久醒来一次检查是否已被取消。
# 0.05 秒远小于“2 秒内交还工作目录”的要求。
_POLL = 0.05


class RateLimiter:
    def __init__(self, min_interval: float) -> None:
        if min_interval < 0:
            raise ValueError("min_interval 不能为负")
        self.min_interval = min_interval
        self._cond = threading.Condition(threading.Lock())
        self._next_allowed = 0.0
        self._pause_until = 0.0
        # 统计计数，供验证与运维观察
        self.dispatched = 0
        self.pause_events = 0
        self.paused_waits = 0

    # ------------------------------------------------------------------ #
    def acquire(self, token: Optional[object] = None) -> None:
        """阻塞直到获得发起一次请求的许可。

        token 若提供，必须是带有 ``check()`` 方法的取消令牌：等待期间会定期
        检查，令牌取消后立即抛该令牌的 Cancelled。
        """
        while True:
            if token is not None:
                token.check()  # type: ignore[attr-defined]
            with self._cond:
                now = time.monotonic()
                gate = max(self._next_allowed, self._pause_until)
                wait = gate - now
                if wait <= 0:
                    self._next_allowed = now + self.min_interval
                    self.dispatched += 1
                    return
                if self._pause_until > now:
                    self.paused_waits += 1
                # 分段等待，保证取消延迟不超过 _POLL
                self._cond.wait(timeout=min(wait, _POLL))

    def pause(self, seconds: float, reason: str = "") -> None:
        """站点要求暂停：在 seconds 秒内统一推迟所有新请求。"""
        if seconds <= 0:
            return
        with self._cond:
            until = time.monotonic() + seconds
            if until > self._pause_until:
                self._pause_until = until
                self.pause_events += 1
                log.warning("站点要求暂停 %.1f 秒，统一推迟新请求（%s）", seconds, reason)
            self._cond.notify_all()

    def wake(self) -> None:
        """叫醒所有等待者重新检查各自的取消令牌 / 门闩状态。"""
        with self._cond:
            self._cond.notify_all()

    # ------------------------------------------------------------------ #
    @property
    def paused_for(self) -> float:
        with self._cond:
            return max(0.0, self._pause_until - time.monotonic())

    def snapshot(self) -> dict:
        with self._cond:
            return {
                "dispatched": self.dispatched,
                "pause_events": self.pause_events,
                "paused_waits": self.paused_waits,
                "min_interval": self.min_interval,
            }
