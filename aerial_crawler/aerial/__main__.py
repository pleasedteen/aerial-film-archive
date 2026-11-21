"""命令行入口。

用法：
  python -m aerial --base-url http://127.0.0.1:8080 --workdir ./data full
  python -m aerial --base-url http://127.0.0.1:8080 --workdir ./data incremental

命令行同样走 Session：
- 前台运行一个会话，SIGINT（Ctrl-C）/ SIGTERM 触发“优雅停采”：
  在 2 秒内交还工作目录，之后不再发请求、不再写文件；
- 停止时保留已 fsync 的区段与已提交的清单，用原工作目录再启动即可续采。
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path

from .config import Config
from .session import Session, SessionConflict, SessionManager
from .state import WorkDir


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="航拍底片扫描件采集器")
    p.add_argument("--base-url", required=True, help="模拟站根地址")
    p.add_argument("--workdir", required=True, type=Path, help="本地工作目录（可续采）")
    p.add_argument("--mode", choices=("full", "incremental"),
                   help="采集模式；也可用位置参数 full/incremental")
    p.add_argument("--workers", type=int, default=4, help="并发 worker 数")
    p.add_argument("--min-interval", type=float, default=0.05,
                   help="全站统一的最小请求发起间隔（秒）")
    p.add_argument("--chunk-size", type=int, default=64 * 1024,
                   help="分段下载区段大小（字节）")
    p.add_argument("--retry-rounds", type=int, default=4, help="补齐轮数")
    p.add_argument("--retry-round-interval", type=float, default=1.0,
                   help="补齐轮之间的等待秒数")
    p.add_argument("--max-retries", type=int, default=6, help="单请求最大重试次数")
    p.add_argument("--stop-timeout", type=float, default=2.0,
                   help="收到停止信号后交还工作目录的时限（秒）")
    p.add_argument("positional_mode", nargs="?", choices=("full", "incremental"))
    return p.parse_args(argv)


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_dir / "run.log", encoding="utf-8"),
            logging.StreamHandler(sys.stderr),
        ],
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    mode = args.mode or args.positional_mode
    if mode is None:
        print("错误：必须指定采集模式 full 或 incremental", file=sys.stderr)
        return 64

    cfg = Config(
        base_url=args.base_url.rstrip("/"),
        workdir=args.workdir,
        workers=args.workers,
        min_interval=args.min_interval,
        chunk_size=args.chunk_size,
        retry_rounds=args.retry_rounds,
        retry_round_interval=args.retry_round_interval,
        max_retries=args.max_retries,
    )
    wd = WorkDir(cfg.workdir)
    setup_logging(wd.log_dir)
    log = logging.getLogger("aerial")

    manager = SessionManager()
    session = manager.create(cfg, mode)
    stop_requested = threading.Event()

    # 信号 → 优雅停采（在信号处理里完成，主线程只等待结果）
    def _on_signal(signum, _frame) -> None:
        log.warning("收到信号 %s，开始优雅停采（≤ %.1f 秒交还工作目录）",
                    signum, args.stop_timeout)
        stop_requested.set()
        session.stop(timeout=args.stop_timeout)

    old_int = signal.signal(signal.SIGINT, _on_signal)
    old_term = signal.signal(signal.SIGTERM, _on_signal)
    try:
        try:
            session.start()
        except SessionConflict as exc:
            log.error("%s", exc)
            return 73  # EX_CANTCREAT 类：资源占用
        # 不用无超时的 join：即使工作线程迟迟未退，收到停止信号后也要能返回
        stop_deadline = None
        while True:
            result = session.wait(0.2)
            if result is not None:
                return result
            if stop_requested.is_set():
                if stop_deadline is None:
                    stop_deadline = time.monotonic() + args.stop_timeout + 2.0
                if not session.running or time.monotonic() >= stop_deadline:
                    # stop() 已交还工作目录锁；线程是守护线程，进程退出不阻塞
                    return 130
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)


if __name__ == "__main__":
    sys.exit(main())
