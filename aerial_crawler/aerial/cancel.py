"""会话级取消令牌。

一个采集会话（Session）持有唯一一个 CancelToken：
- 所有可能长时间阻塞的位置（连接、等响应头、读响应体、限流暂停、退避
  睡眠、补齐轮次等待、worker 队列等待）都必须周期性检查它；
- 令牌被取消后，任何新的文件写入都会立即抛 Cancelled，保证“停采后不再
  发请求、不再写文件”。

阻塞 I/O 的立即中止
-------------------
纯 Python 的 ``socket.recv`` 在另一个线程里只置一个事件是叫不醒的，所以
令牌还维护一组“关闭器”（closer）：发起请求前把当前连接注册进来，取消时
逐个 ``shutdown(SHUT_RDWR)``，阻塞中的 connect / getresponse / read 会立刻
抛 ``OSError``，调用方发现令牌已取消后转成 ``Cancelled`` 向上抛。

写入门闩（write gate）
----------------------
每个写文件动作都用 ``with token.writing():`` 包起来。会话停止时先取消令牌，
再调用 ``close_writes()`` 合上闩并等待在途写入结束。合上之后任何写入尝试都
会抛 Cancelled，即使工作目录锁已经交还。
"""
from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator


class Cancelled(Exception):
    """会话已被停止：调用方必须立刻放弃，不得再发请求或写文件。"""


class CancelToken:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._gate = threading.RLock()
        self._idle = threading.Condition(self._gate)
        self._active_writes = 0
        self._closed = False
        self._closers: set[Callable[[], None]] = set()

    # ------------------------------------------------------------------ #
    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        self._event.set()
        # 立刻叫醒阻塞在 socket 上的请求
        with self._gate:
            closers = list(self._closers)
        for closer in closers:
            try:
                closer()
            except Exception:  # noqa: BLE001 - 关闭器必须互不影响
                pass

    def check(self) -> None:
        if self._event.is_set():
            raise Cancelled()

    def sleep(self, seconds: float) -> None:
        """可取消的 sleep：被取消则立刻抛 Cancelled。"""
        if seconds <= 0:
            self.check()
            return
        if self._event.wait(seconds):
            raise Cancelled()
        self.check()

    # ------------------------------------------------------------------ #
    def add_closer(self, closer: Callable[[], None]) -> None:
        with self._gate:
            if self._event.is_set():
                # 注册时已取消：立即关闭，避免竞态漏掉
                try:
                    closer()
                except Exception:  # noqa: BLE001
                    pass
                raise Cancelled()
            self._closers.add(closer)

    def remove_closer(self, closer: Callable[[], None]) -> None:
        with self._gate:
            self._closers.discard(closer)

    @staticmethod
    def socket_closer(conn: object) -> Callable[[], None]:
        """构造一个关闭 http.client 连接的 closer（无阻塞、幂等）。"""
        def _close() -> None:
            sock = getattr(conn, "sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
        return _close

    # ------------------------------------------------------------------ #
    @contextmanager
    def writing(self) -> Iterator[None]:
        """进入一次文件写入。会话已停止时直接拒绝。"""
        with self._gate:
            if self._closed or self._event.is_set():
                raise Cancelled()
            self._active_writes += 1
        try:
            yield
        finally:
            with self._gate:
                self._active_writes -= 1
                if self._active_writes == 0:
                    self._idle.notify_all()

    def close_writes(self, timeout: float) -> bool:
        """合上写入门闩并等待在途写入结束；返回是否已全部排空。"""
        with self._gate:
            self._closed = True
            deadline = time.monotonic() + max(0.0, timeout)
            while self._active_writes > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._idle.wait(remaining)
            return True

    @property
    def writes_closed(self) -> bool:
        with self._gate:
            return self._closed

    @property
    def active_writes(self) -> int:
        with self._gate:
            return self._active_writes
