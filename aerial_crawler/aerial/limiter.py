"""全站共享的请求限流器。

约束：
1. 所有发往站点的请求（目录页、详情、影像区段）在发起前都必须先 acquire()，
   保证任意两次请求发起之间至少相隔 min_interval 秒——无论多少个 worker 并发。
2. 站点返回 503/429 并带 Retry-After 时，调用 pause() 统一推迟“所有”新请求，
   而不是只让命中的那一个 worker 睡眠。
"""
from __future__ import annotations

import logging
import threading
import time

log = logging.getLogger(__name__)


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

    def acquire(self) -> None:
        """阻塞直到获得发起一次请求的许可。"""
        with self._cond:
            while True:
                now = time.monotonic()
                gate = max(self._next_allowed, self._pause_until)
                wait = gate - now
                if wait <= 0:
                    self._next_allowed = now + self.min_interval
                    self.dispatched += 1
                    return
                if self._pause_until > now:
                    self.paused_waits += 1
                self._cond.wait(timeout=wait)

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

    def snapshot(self) -> dict:
        with self._cond:
            return {
                "dispatched": self.dispatched,
                "pause_events": self.pause_events,
                "paused_waits": self.paused_waits,
                "min_interval": self.min_interval,
            }
