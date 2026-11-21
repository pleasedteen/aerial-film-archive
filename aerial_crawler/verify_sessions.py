#!/usr/bin/env python3
"""会话级端到端验证：并行采集、可独立停止、停采 2 秒内交还工作目录、接续。

用本地模拟站（同进程内起两个互不干扰的站）验证：

  A. 等待目录响应时停采
  B. 等待站点暂停（503 + Retry-After）时停采
  C. 等待影像响应体时停采
     以上三种情况都断言：
       - Session.stop() 在 2 秒内返回；
       - 返回后工作目录锁已交还（另一会话能立即占用）；
       - 该会话之后不再发出任何请求（模拟站请求计数不增长）；
       - 该会话之后不再写文件（events/current/state/区段 mtime+size 不变）；
  D. 另一站点的采集不受影响（继续推进并正常结束，exit=0）；
  E. 被停的会话用原工作目录重新启动：复用已落盘区段、只补缺档；
  F. 清单只含完整且校验通过的影像（逐卷重算 sha256 / 大小），
     且不包含停止时尚未完成的半成品。

用法：
  python3 verify_sessions.py            # 自动端口、临时目录，约 40 秒
  python3 verify_sessions.py --keep     # 保留现场
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aerial.config import Config
from aerial.session import SessionConflict, SessionManager
from mocksite.server import build_server


# ---------------------------------------------------------------------- #
class Check:
    def __init__(self) -> None:
        self.items: list[tuple[str, bool, str]] = []

    def expect(self, name: str, ok: bool, detail: str = "") -> bool:
        self.items.append((name, bool(ok), detail))
        print(f"  [{'✓' if ok else '✗'}] {name}" + (f" —— {detail}" if detail else ""))
        return bool(ok)

    @property
    def all_ok(self) -> bool:
        return all(ok for _, ok, _ in self.items)


def free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_station(rate: int = 20):
    port = free_port()
    server = build_server("127.0.0.1", port, rate)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, f"http://127.0.0.1:{port}"


def stop_station(server) -> None:
    server.control.release_all()
    server.shutdown()
    server.server_close()


def make_cfg(base_url: str, workdir: Path, **kw) -> Config:
    defaults = dict(
        base_url=base_url, workdir=workdir, workers=3,
        min_interval=0.02, chunk_size=4096,
        retry_rounds=8, retry_round_interval=1.0, max_retries=4,
    )
    defaults.update(kw)
    return Config(**defaults)


def configure_session_logging() -> None:
    """程序内建的会话也要写 workdir/logs/run.log（与命令行一致的生产日志）。

    Session 本身不装日志；这里给每个会话的日志统一挂上文件 handler。
    """
    import logging
    root = logging.getLogger("aerial")
    root.setLevel(logging.INFO)
    root.propagate = False


def attach_log_file(log_path: Path) -> logging.Handler:
    import logging
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    logging.getLogger("aerial").addHandler(handler)
    return handler


def snapshot_workdir(workdir: Path) -> dict[str, tuple[int, int, int]]:
    """记录工作目录内所有文件的 (size, mtime_ns, inode)，用于断言不再写文件。"""
    snap: dict[str, tuple[int, int, int]] = {}
    if not workdir.exists():
        return snap
    for p in workdir.rglob("*"):
        try:
            if p.is_file():
                st = p.stat()
                snap[str(p.relative_to(workdir))] = (st.st_size, st.st_mtime_ns, st.st_ino)
        except OSError:
            pass
    return snap


def wait_until(pred, timeout: float, step: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def verify_images(check: Check, workdir: Path, label: str) -> None:
    cur_path = workdir / "manifest" / "current.json"
    cur = json.loads(cur_path.read_text("utf-8")) if cur_path.exists() else {}
    bad = []
    for vid, e in cur.items():
        p = Path(e["image_path"])
        if not p.exists():
            bad.append(f"{vid} 文件缺失")
            continue
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        if digest != e["checksum"] or p.stat().st_size != e["size"]:
            bad.append(f"{vid} 校验不符")
    check.expect(f"{label}：清单中每卷影像均完整且 sha256/大小通过",
                 not bad, "; ".join(bad) or f"{len(cur)} 卷全部通过")
    # current 里引用的必然是 *.img 完整件，不能是区段或 .assembling
    wrong = [Path(e["image_path"]).name for e in cur.values()
             if not str(e["image_path"]).endswith(".img")
             or ".assembling" in str(e["image_path"])]
    check.expect(f"{label}：清单只指向完整影像（无区段/半成品）", not wrong,
                 ", ".join(wrong) or "全部为 *.img")


# ---------------------------------------------------------------------- #
def scenario_catalog_hold(check: Check, tmp: Path, manager: SessionManager) -> None:
    """A. 等待目录响应时停采。"""
    print("\n[A] 等待目录响应时停采")
    server, base = start_station()
    log_handler = None
    try:
        wd = tmp / "A"
        log_handler = attach_log_file(wd / "logs" / "run.log")
        cfg = make_cfg(base, wd)
        server.control.hold_catalog()          # 服务端收到目录请求后挂起
        sess = manager.create(cfg, "full", name="A").start()

        # 等到客户端确实发出目录请求并被服务端挂起
        arrived = wait_until(lambda: server.control.request_count("/catalog") >= 1, 5.0)
        check.expect("客户端已发出目录请求并阻塞在等服务端响应", arrived)
        before_count = server.control.request_count()

        t0 = time.monotonic()
        sess.stop(timeout=2.0)
        elapsed = time.monotonic() - t0
        check.expect("停采在 2 秒内返回", elapsed <= 2.0, f"{elapsed:.2f}s")
        check.expect("工作目录锁已交还", _lock_free(wd))

        # stop 之后立刻取快照，释放服务端阻塞后再比对：该会话不得写文件
        snap = snapshot_workdir(wd)
        server.control.release_catalog()
        time.sleep(0.8)
        check.expect("停采后不再发出任何请求",
                     server.control.request_count() == before_count,
                     f"{before_count} → {server.control.request_count()}")
        changed = _diff(snap, snapshot_workdir(wd))
        check.expect("停采后不再写文件", not changed, ", ".join(changed))
    finally:
        if log_handler is not None:
            _detach_log_file(log_handler)
        stop_station(server)


def scenario_pause_hold(check: Check, tmp: Path, manager: SessionManager) -> None:
    """B. 等待站点暂停（503 + Retry-After）时停采。"""
    print("\n[B] 等待站点统一暂停时停采")
    server, base = start_station()
    log_handler = None
    try:
        wd = tmp / "B"
        log_handler = attach_log_file(wd / "logs" / "run.log")
        cfg = make_cfg(base, wd)
        # 第一次目录请求返回 503 + Retry-After=30：采集端进入统一暂停
        server.control.force_status("/catalog", 503, 30.0, times=1)
        sess = manager.create(cfg, "full", name="B").start()

        wait_until(lambda: server.control.request_count("/catalog") >= 1, 5.0)
        time.sleep(0.3)   # 让客户端处理 503 并进入 limiter 暂停
        before_count = server.control.request_count()

        t0 = time.monotonic()
        sess.stop(timeout=2.0)
        elapsed = time.monotonic() - t0
        check.expect("停采在 2 秒内返回（即使站点要求暂停 30 秒）", elapsed <= 2.0,
                     f"{elapsed:.2f}s")
        check.expect("工作目录锁已交还", _lock_free(wd))

        snap = snapshot_workdir(wd)
        server.control.release_all()
        time.sleep(0.8)
        check.expect("停采后不再发出任何请求",
                     server.control.request_count() == before_count,
                     f"{before_count} → {server.control.request_count()}")
        changed = _diff(snap, snapshot_workdir(wd))
        check.expect("停采后不再写文件", not changed, ", ".join(changed))
    finally:
        if log_handler is not None:
            _detach_log_file(log_handler)
        stop_station(server)


def scenario_image_body_hold(check: Check, tmp: Path, manager: SessionManager) -> None:
    """C. 等待影像响应体时停采。"""
    print("\n[C] 等待影像响应体时停采")
    server, base = start_station()
    log_handler = None
    try:
        wd = tmp / "C"
        log_handler = attach_log_file(wd / "logs" / "run.log")
        cfg = make_cfg(base, wd, workers=2, chunk_size=8192)
        server.control.arm_image_body_hold(after_bytes=4096, nth=1)
        sess = manager.create(cfg, "full", name="C").start()

        claimed = server.control.image_hold_claimed.wait(10.0)
        check.expect("客户端已收到影像响应头并阻塞在读响应体", claimed)
        # 确认确有区段开始落盘（说明下载确实在进行）
        wait_until(lambda: len(_completed_chunks(wd)) >= 1, 8.0)
        before_count = server.control.request_count()

        t0 = time.monotonic()
        sess.stop(timeout=2.0)
        elapsed = time.monotonic() - t0
        check.expect("停采在 2 秒内返回", elapsed <= 2.0, f"{elapsed:.2f}s")
        check.expect("工作目录锁已交还", _lock_free(wd))

        snap = snapshot_workdir(wd)
        server.control.release_image_body()
        time.sleep(0.8)
        check.expect("停采后不再发出任何请求",
                     server.control.request_count() == before_count,
                     f"{before_count} → {server.control.request_count()}")
        changed = _diff(snap, snapshot_workdir(wd))
        check.expect("停采后不再写文件（含当前阻塞的区段）", not changed,
                     ", ".join(changed))
        verify_images(check, wd, "C 续采前")
    finally:
        if log_handler is not None:
            _detach_log_file(log_handler)
        stop_station(server)


def scenario_parallel_isolation(check: Check, tmp: Path, manager: SessionManager) -> None:
    """D. 另一站点采集不受影响 + 原目录接续。"""
    print("\n[D] 两站并行：停一个站，另一个站继续；随后原目录接续")
    server_a, base_a = start_station()
    server_b, base_b = start_station()
    log_a = log_b = None
    try:
        wd_a = tmp / "D_a"
        wd_b = tmp / "D_b"
        log_a = attach_log_file(wd_a / "logs" / "run.log")
        log_b = attach_log_file(wd_b / "logs" / "run.log")
        # 挂起第 2 个数据响应：第 1 段先完整落盘，停采后必有可复用区段
        server_a.control.arm_image_body_hold(after_bytes=4096, nth=2)
        sa = manager.create(make_cfg(base_a, wd_a, chunk_size=8192), "full",
                            name="D-a").start()
        sb = manager.create(make_cfg(base_b, wd_b, chunk_size=8192), "full",
                            name="D-b").start()

        claimed = server_a.control.image_hold_claimed.wait(10.0)
        check.expect("A 站客户端阻塞在影像响应体", claimed)
        # 等 A 站至少有一段完整区段落盘，确保停采后确有可复用区段
        chunk_ready = wait_until(lambda: len(_completed_chunks(wd_a)) >= 1, 10.0)
        check.expect("A 站停采前已有完整区段落盘", chunk_ready,
                     f"{len(_completed_chunks(wd_a))} 段")
        b_progress = wait_until(lambda: _current_count(wd_b) >= 1, 15.0)
        check.expect("A 站阻塞时 B 站仍在推进采集", b_progress,
                     f"B 清单 {_current_count(wd_b)} 卷")

        b_count_before = server_b.control.request_count()
        b_cur_before = _current_count(wd_b)
        t0 = time.monotonic()
        sa.stop(timeout=2.0)
        a_elapsed = time.monotonic() - t0
        check.expect("A 站会话停采在 2 秒内返回", a_elapsed <= 2.0, f"{a_elapsed:.2f}s")

        b_more = wait_until(
            lambda: server_b.control.request_count() > b_count_before, 5.0)
        check.expect("停 A 后 B 站仍有新请求（不受影响）", b_more,
                     f"{b_count_before} → {server_b.control.request_count()}")
        b_exit = sb.wait(timeout=60.0)
        # B 站独立全量：V0007 的影像要更晚才开放，此时可能留在失败队列并以
        # exit=2 正常返回（状态已落盘，再跑一次即可续）。只要不是异常退出即可。
        check.expect("B 站会话按约定结束（0=完成 / 2=仍有待补齐）",
                     b_exit in (0, 2), f"exit={b_exit}")
        verify_images(check, wd_b, "B 站")

        a_chunks = _completed_chunks(wd_a)
        check.expect("A 站停采后仍有已落盘区段可供续采", len(a_chunks) >= 1,
                     f"{len(a_chunks)} 段")
        interrupted_vids = {_volume_of_chunk(c, wd_a) for c in a_chunks}

        # 原目录再启动（全量）：复用区段、补全当前可见卷宗。
        # 区段大小必须与中断那次一致，否则区段边界不同无法复用。
        server_a.control.release_image_body()
        mark = _log_line_count(wd_a / "logs")
        sa2 = manager.create(make_cfg(base_a, wd_a, chunk_size=8192), "full",
                             name="D-a2").start()
        a_exit = sa2.wait(timeout=90.0)
        check.expect("A 站原目录接续正常结束（exit=0）", a_exit == 0, f"exit={a_exit}")
        cur_a = _current_ids(wd_a)
        check.expect("A 站接续后，被中断的卷宗已完整入库",
                     interrupted_vids and interrupted_vids <= cur_a,
                     f"中断卷 {sorted(interrupted_vids)} ⊆ 清单 {sorted(cur_a)}")
        check.expect("A 站接续后清单已覆盖全部当前可见卷宗",
                     len(cur_a - {"V0007"}) >= 7,
                     f"{sorted(cur_a)}")
        verify_images(check, wd_a, "A 站接续后")
        reuse = _reuse_counts_since(wd_a / "logs", mark)
        check.expect("接续日志显示复用了已落盘区段", bool(reuse) and max(reuse) >= 1,
                     f"各卷复用数 {reuse}")
    finally:
        for h in (log_a, log_b):
            if h is not None:
                _detach_log_file(h)
        stop_station(server_a)
        stop_station(server_b)


def scenario_resume_reuses_chunks(check: Check, tmp: Path, manager: SessionManager) -> None:
    """E. 停采 → 原目录重启 → 复用区段只补缺档。"""
    print("\n[E] 停采后原目录重启复用已落盘区段")
    server, base = start_station()
    log_handler = None
    try:
        wd = tmp / "E"
        log_handler = attach_log_file(wd / "logs" / "run.log")
        cfg = make_cfg(base, wd, chunk_size=8192)
        # 挂起第 2 个数据响应：第 1 段先完整落盘，停采后必有可复用区段
        server.control.arm_image_body_hold(after_bytes=4096, nth=2)
        sess = manager.create(cfg, "full", name="E").start()
        check.expect("客户端阻塞在影像响应体", server.control.image_hold_claimed.wait(10.0))
        chunk_ready = wait_until(lambda: len(_completed_chunks(wd)) >= 1, 8.0)
        check.expect("停采前已有完整区段落盘", chunk_ready,
                     f"{len(_completed_chunks(wd))} 段")
        chunks_before = len(_completed_chunks(wd))
        sess.stop(timeout=2.0)
        server.control.release_image_body()
        check.expect("停采时已有区段落盘", chunks_before >= 1, f"{chunks_before} 段")
        chunks = _completed_chunks(wd)
        interrupted_vids = {_volume_of_chunk(c, wd) for c in chunks}

        # 原目录重启（全量）：断言被中断卷宗的区段被复用
        mark = _log_line_count(wd / "logs")
        sess2 = manager.create(cfg, "full", name="E2").start()
        exit_code = sess2.wait(timeout=90.0)
        check.expect("原目录续采正常结束（exit=0）", exit_code == 0, f"exit={exit_code}")
        reuse = _reuse_counts_since(wd / "logs", mark)
        check.expect("续采实际复用了已落盘区段（≥1）", bool(reuse) and max(reuse) >= 1,
                     f"各卷复用数 {reuse}")
        cur = _current_ids(wd)
        check.expect("被中断的卷宗最终完整入库", interrupted_vids and interrupted_vids <= cur,
                     f"中断卷 {sorted(interrupted_vids)} ⊆ 清单 {sorted(cur)}")
        verify_images(check, wd, "E 续采后")
    finally:
        if log_handler is not None:
            _detach_log_file(log_handler)
        stop_station(server)


def scenario_cli(check: Check, tmp: Path) -> None:
    """G. 命令行入口也走会话：SIGTERM 优雅停采并交还目录。"""
    print("\n[G] 命令行入口走会话：SIGTERM 停采")
    import signal
    import subprocess
    server, base = start_station()
    try:
        wd = tmp / "G"
        server.control.hold_catalog()
        proc = subprocess.Popen(
            [sys.executable, "-m", "aerial",
             "--base-url", base, "--workdir", str(wd),
             "--workers", "2", "--min-interval", "0.02",
             "--chunk-size", "8192", "full"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        arrived = wait_until(lambda: server.control.request_count("/catalog") >= 1, 8.0)
        check.expect("命令行进程已发出目录请求并阻塞", arrived)
        before = server.control.request_count()

        t0 = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=4.0)
        except subprocess.TimeoutExpired:
            proc.kill()
        elapsed = time.monotonic() - t0
        check.expect("SIGTERM 后进程在 2 秒左右退出并交还目录", elapsed <= 3.0,
                     f"{elapsed:.2f}s exit={proc.returncode}")
        check.expect("命令行进程退出后工作目录锁已释放", _lock_free(wd))

        snap = snapshot_workdir(wd)
        server.control.release_catalog()
        time.sleep(0.6)
        check.expect("命令行进程停采后不再发出请求",
                     server.control.request_count() == before,
                     f"{before} → {server.control.request_count()}")
        changed = _diff(snap, snapshot_workdir(wd))
        check.expect("命令行进程停采后不再写文件", not changed, ", ".join(changed))
    finally:
        stop_station(server)


# ---------------------------------------------------------------------- #
def attach_log_file(log_path: Path):
    import logging
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger("aerial")
    root.setLevel(logging.INFO)
    root.propagate = False
    root.addHandler(handler)
    return handler


def _detach_log_file(handler) -> None:
    import logging
    logging.getLogger("aerial").removeHandler(handler)
    handler.close()


def _diff(before: dict, after: dict) -> list[str]:
    """返回在 before 与 after 之间发生变化（含新增/删除）的相对路径。"""
    changed = []
    for key in set(before) | set(after):
        if before.get(key) != after.get(key):
            changed.append(key)
    return sorted(changed)


def _lock_free(wd: Path) -> bool:
    """尝试以非阻塞方式占用工作目录锁，成功即说明已交还。"""
    from aerial.state import WorkDir
    if not wd.exists():
        return True
    probe = WorkDir(wd)
    try:
        probe.acquire_lock()
    except RuntimeError:
        return False
    else:
        probe.release_lock()
        return True


def _completed_chunks(wd: Path) -> list[Path]:
    """已完整落盘的区段（排除 .part 半成品）。"""
    if not wd.exists():
        return []
    return [p for p in wd.rglob("chunk_*") if not p.name.endswith(".part")]


def _current_count(wd: Path) -> int:
    p = wd / "manifest" / "current.json"
    if not p.exists():
        return 0
    try:
        return len(json.loads(p.read_text("utf-8")))
    except (json.JSONDecodeError, OSError):
        return 0


def _current_ids(wd: Path) -> set[str]:
    p = wd / "manifest" / "current.json"
    if not p.exists():
        return set()
    try:
        return set(json.loads(p.read_text("utf-8")).keys())
    except (json.JSONDecodeError, OSError):
        return set()


def _volume_of_chunk(chunk_path: Path, wd: Path) -> str:
    """区段路径 images/<vid>/chunks/<tag>/chunk_N → 卷宗号。"""
    rel = chunk_path.relative_to(wd / "images")
    return rel.parts[0]


def _log_line_count(log_dir: Path) -> int:
    p = log_dir / "run.log"
    if not p.exists():
        return 0
    return len(p.read_text("utf-8", errors="replace").splitlines())


def _reuse_counts_since(log_dir: Path, start_line: int) -> list[int]:
    import re
    p = log_dir / "run.log"
    if not p.exists():
        return []
    lines = p.read_text("utf-8", errors="replace").splitlines()[start_line:]
    return [int(m) for m in re.findall(r"复用 (\d+) 段", "\n".join(lines))]


# ---------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", type=Path, default=None)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    tmp = args.workdir or Path(tempfile.mkdtemp(prefix="aerial_sessions_"))
    tmp.mkdir(parents=True, exist_ok=True)
    check = Check()
    manager = SessionManager()
    started = time.monotonic()

    print(f"== 会话级端到端验证（工作目录 {tmp}）==")
    try:
        scenario_catalog_hold(check, tmp, manager)
        scenario_pause_hold(check, tmp, manager)
        scenario_image_body_hold(check, tmp, manager)
        scenario_parallel_isolation(check, tmp, manager)
        scenario_resume_reuses_chunks(check, tmp, manager)
        scenario_cli(check, tmp)
    finally:
        elapsed = time.monotonic() - started
        print("\n================ 验证报告 ================")
        passed = sum(1 for _, ok, _ in check.items if ok)
        for name, ok, _detail in check.items:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        print(f"合计 {passed}/{len(check.items)} 通过，用时 {elapsed:.1f} 秒")
        if not args.keep and args.workdir is None:
            shutil.rmtree(tmp, ignore_errors=True)
        elif args.keep or args.workdir:
            print(f"现场保留于 {tmp}")

    return 0 if check.all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
