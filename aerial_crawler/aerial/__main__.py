"""命令行入口。

用法：
  python -m aerial --base-url http://127.0.0.1:8080 --workdir ./data full
  python -m aerial --base-url http://127.0.0.1:8080 --workdir ./data incremental
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import Config
from .scanner import Scanner
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
    p.add_argument("--backfill-seconds", type=int, default=600,
                   help="补登窗口（站点秒）：条目最迟在更新时间后多久上目录")
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
        backfill_seconds=args.backfill_seconds,
    )
    wd = WorkDir(cfg.workdir)
    setup_logging(wd.log_dir)
    log = logging.getLogger("aerial")
    try:
        wd.acquire_lock()
    except RuntimeError as exc:
        log.error("%s", exc)
        return 73  # EX_CANTCREAT 类：资源占用

    try:
        scanner = Scanner(cfg, wd, mode)
        return scanner.run()
    except KeyboardInterrupt:
        log.warning("收到中断信号，已落盘的区段和清单可用于续采")
        return 130
    finally:
        wd.release_lock()


if __name__ == "__main__":
    sys.exit(main())
