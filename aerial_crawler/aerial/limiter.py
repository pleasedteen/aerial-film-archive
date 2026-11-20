"""全站共享的请求限流器（按请求头实际发出时刻计时）。

约束：
1. 所有发往站点的请求（目录页、详情、影像区段，以及它们的重试）在“请求头
   实际发出”之前都必须经过同一个限流器，保证任意两次请求头发送之间至少相隔
   min_interval 秒——无论多少个 worker 并发、也无论请求属于哪个阶段。
2. 慢建连（connect 阻塞）与慢响应体（read 阻塞）发生在票号等待之外，
   不占用队首、不挡住其他已就绪 worker 的请求。
3. 站点返回 503/429 并带 Retry-After 时，pause() 统一推迟“所有”新请求，
   而不是只让命中的那一个 worker 睡眠；即使命中请求已用完重试次数，
   暂停依然生效。

票号生命周期：

    reserve()          建连之前取票，立即返回（只做时刻预约，不阻塞）
        |
    connect()          慢建连发生在这里，与限流器无关，不挡别人
        |
    wait_turn()        阻塞到本票成为队首且到达放行时刻（含统一暂停）
        |              —— 调用方随即发送请求头，此刻即“实际发出时刻”
    getresponse/read() 慢响应体发生在这里，票号已离队，不挡别人
        |
    release()          响应结束（或建连失败）后归还，仅用于在途统计
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Optional

log = logging.getLogger(__name__)


class Ticket:
    """一次请求的预约票号；每个请求（含每次重试）各取一张。"""

    __slots__ = ("idx", "gate", "sent_at", "released", "paused")

    def __init__(self, idx: int, gate: float) -> None:
        self.idx = idx
        # 预约的最早放行时刻（monotonic）
        self.gate = gate
        # 请求头实际发出时刻（monotonic）；未发出为 None
        self.sent_at: Optional[float] = None
        self.released = False
        # 等待放行期间是否撞上过统一暂停窗口
        self.paused = False


class RateLimiter:
    def __init__(self, min_interval: float) -> None:
        if min_interval < 0:
            raise ValueError("min_interval 不能为负")
        self.min_interval = min_interval
        self._cond = threading.Condition(threading.Lock())
        # 等待放行的票号（FIFO）；发出即出队，慢响应体不留在队列里
        self._queue: deque[Ticket] = deque()
        # 已发出但尚未 release 的票号（仅用于在途观测）
        self._inflight: set[Ticket] = set()
        # 最近一张预约票的放行时刻，用于把新票号串成等间隔链条
        self._tail_gate = 0.0
        # 最近一次请求头实际发出的时刻
        self._last_sent: Optional[float] = None
        self._pause_until = 0.0
        self._next_idx = 0
        # 统计计数，供验证与运维观察
        self.dispatched = 0
        self.pause_events = 0
        self.paused_waits = 0

    # ------------------------------------------------------------------ #
    def reserve(self) -> Ticket:
        """预约一次请求的票号，立即返回。

        可以在（慢）建连之前调用：取票本身从不阻塞，建连耗时不会被计入
        限流间隔，也不会挡住其他 worker。
        """
        with self._cond:
            now = time.monotonic()
            gate = max(
                now,
                self._pause_until,
                self._tail_gate + self.min_interval,
            )
            ticket = Ticket(self._next_idx, gate)
            self._next_idx += 1
            self._queue.append(ticket)
            self._tail_gate = gate
            if now < self._pause_until:
                ticket.paused = True
            return ticket

    def wait_turn(self, ticket: Ticket) -> None:
        """阻塞到本票可以发出请求头（返回后调用方应立即发送请求头）。

        放行条件（全部满足）：
          - 本票位于队首（严格 FIFO，后取票不能插头发送）；
          - 到达自身预约时刻；
          - 距上一次实际发出至少 min_interval；
          - 不在统一暂停窗口内。
        """
        with self._cond:
            while True:
                now = time.monotonic()
                if self._queue and self._queue[0] is ticket:
                    gate = ticket.gate
                    if self._last_sent is not None:
                        gate = max(gate, self._last_sent + self.min_interval)
                    gate = max(gate, self._pause_until)
                    if gate <= now:
                        # 立即出队：之后即便响应体很慢也不阻塞后续票号
                        self._queue.popleft()
                        ticket.sent_at = now
                        self._last_sent = now
                        self._inflight.add(ticket)
                        self.dispatched += 1
                        return
                    if self._pause_until > now:
                        self.paused_waits += 1
                        ticket.paused = True
                    self._cond.wait(timeout=gate - now)
                else:
                    # 等队首变化（发出 / 放弃）或新的暂停通知
                    self._cond.wait()

    def release(self, ticket: Ticket) -> None:
        """请求结束（含建连失败、未发出即放弃）后归还票号。"""
        with self._cond:
            if ticket.released:
                return
            ticket.released = True
            try:
                self._queue.remove(ticket)
            except ValueError:
                self._inflight.discard(ticket)
            self._cond.notify_all()

    def acquire(self) -> Ticket:
        """兼容入口：取票并立即等待放行（无建连阶段的场景/测试使用）。"""
        ticket = self.reserve()
        self.wait_turn(ticket)
        return ticket

    # ------------------------------------------------------------------ #
    def pause(self, seconds: float, reason: str = "") -> None:
        """站点要求暂停：在 seconds 秒内统一推迟所有新请求。

        已在途（请求头已发出、正在等响应/读响应体）的请求不受影响，
        但暂停期间任何 worker 都不能发出新的请求头。
        """
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
                "inflight": len(self._inflight),
                "waiting": len(self._queue),
                "min_interval": self.min_interval,
            }
