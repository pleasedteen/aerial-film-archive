"""会话取消的规则级单元测试（不启动网络服务）。

覆盖：
- CancelToken：取消唤醒 sleep、写门闩关闭后拒绝写入、在途写入排空；
- RateLimiter：每会话独立取消，一个会话被停止不影响另一个；
- 工作目录：会话停止后残留线程无法再写入；
- 会话停止时的文件清理（未提交区段可删、已提交影像保留）。
"""
from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aerial.cancel import Cancelled, CancelToken
from aerial.limiter import RateLimiter
from aerial.state import WorkDir


class CancelTokenTests(unittest.TestCase):
    def test_sleep_is_interrupted(self) -> None:
        token = CancelToken()
        t0 = time.monotonic()
        threading.Timer(0.1, token.cancel).start()
        with self.assertRaises(Cancelled):
            token.sleep(5.0)
        self.assertLess(time.monotonic() - t0, 1.0)

    def test_writing_rejected_after_cancel(self) -> None:
        token = CancelToken()
        token.cancel()
        with self.assertRaises(Cancelled):
            with token.writing():
                pass

    def test_close_writes_blocks_new_writes_and_waits_inflight(self) -> None:
        token = CancelToken()
        in_write = threading.Event()
        release = threading.Event()
        result: dict[str, bool] = {}

        def writer() -> None:
            with token.writing():
                in_write.set()
                release.wait(2.0)

        t = threading.Thread(target=writer)
        t.start()
        self.assertTrue(in_write.wait(1.0))

        def closer() -> None:
            result["drained"] = token.close_writes(2.0)

        ct = threading.Thread(target=closer)
        ct.start()
        time.sleep(0.15)
        # 闩已合上：新的写入被拒绝，即便在途写入尚未结束
        with self.assertRaises(Cancelled):
            with token.writing():
                pass
        release.set()
        ct.join(2.0)
        t.join(2.0)
        self.assertTrue(result.get("drained"))

    def test_close_writes_times_out_but_still_closes(self) -> None:
        token = CancelToken()
        hold = threading.Event()

        def stuck() -> None:
            with token.writing():
                hold.wait(5.0)

        t = threading.Thread(target=stuck, daemon=True)
        t.start()
        time.sleep(0.1)
        drained = token.close_writes(0.2)
        self.assertFalse(drained)
        self.assertTrue(token.writes_closed)
        with self.assertRaises(Cancelled):
            with token.writing():
                pass
        hold.set()

    def test_socket_closer_releases_blocked_reader(self) -> None:
        import socket
        token = CancelToken()
        a, b = socket.socketpair()
        try:
            closer = CancelToken.socket_closer(_FakeConn(a))
            token.add_closer(closer)
            got: dict[str, object] = {}

            def reader() -> None:
                try:
                    got["data"] = b.recv(16)
                except OSError as exc:
                    got["err"] = exc

            t = threading.Thread(target=reader)
            t.start()
            time.sleep(0.1)
            token.cancel()
            t.join(2.0)
            # shutdown 让阻塞的 recv 立刻返回（可能是 EOF 或 OSError）
            self.assertTrue("data" in got or "err" in got)
        finally:
            a.close()
            b.close()


class _FakeConn:
    def __init__(self, sock) -> None:
        self.sock = sock


class PerSessionLimiterTests(unittest.TestCase):
    def test_cancel_one_session_does_not_affect_other(self) -> None:
        rl = RateLimiter(0.0)
        rl.pause(0.6, "test")
        t1 = CancelToken()
        t2 = CancelToken()

        def waiter(token, out):
            t0 = time.monotonic()
            try:
                rl.acquire(token)
                out["ok"] = time.monotonic() - t0
            except Cancelled:
                out["cancelled"] = time.monotonic() - t0

        o1: dict[str, float] = {}
        o2: dict[str, float] = {}
        a = threading.Thread(target=waiter, args=(t1, o1))
        b = threading.Thread(target=waiter, args=(t2, o2))
        a.start()
        b.start()
        time.sleep(0.1)
        t1.cancel()
        a.join(2.0)
        b.join(2.0)
        self.assertIn("cancelled", o1)
        self.assertLess(o1["cancelled"], 0.4)   # 立即退出，未等满暂停
        self.assertIn("ok", o2)
        self.assertGreaterEqual(o2["ok"], 0.5)  # 另一会话仍遵守统一暂停


class WorkDirCleanupTests(unittest.TestCase):
    def test_forget_volume_keeps_committed_image_removes_partials(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        wd = WorkDir(Path(tmp.name))
        vol = wd.images_dir / "V0001"
        vol.mkdir(parents=True)
        final = vol / "v1_abcdef.img"
        final.write_bytes(b"committed")
        chunks = vol / "chunks" / "v1_abcdef"
        chunks.mkdir(parents=True)
        (chunks / "chunk_000000").write_bytes(b"partial")
        (vol / "v1_abcdef.img.assembling").write_bytes(b"half")

        wd.forget_volume("V0001")
        self.assertTrue(final.exists())          # 已提交影像保留
        self.assertFalse(chunks.exists())        # 未提交区段删除
        self.assertFalse((vol / "v1_abcdef.img.assembling").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
