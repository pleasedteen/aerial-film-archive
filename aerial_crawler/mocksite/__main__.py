"""模拟站启动入口。

用法：
  python -m mocksite --host 127.0.0.1 --port 8080
  python -m mocksite --port 8080 --rate 20
"""
from __future__ import annotations

import argparse
import logging
import sys

from .scenario import DEFAULT_RATE
from .server import build_server


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="可回放的航拍底片模拟站点")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--rate", type=int, default=DEFAULT_RATE,
                   help="虚拟时钟相对真实时间的速率（默认 20 倍）")
    p.add_argument("--history-pages", type=int, default=0,
                   help="改用大目录场景：历史卷宗占满 N 页，另有 3 处变化")
    p.add_argument("--start-site-time", type=int, default=None,
                   help="虚拟时钟起点（默认虚拟纪元 1700000000）")
    p.add_argument("--catalog-delay", type=float, default=0.0,
                   help="每个目录页响应前的人为延迟（真实秒，用于中断恢复验证）")
    p.add_argument("--last-scan-offset", type=int, default=600,
                   help="大目录场景：上一轮扫描水位的偏移（默认 600）")
    p.add_argument("--missed-offset", type=int, default=480,
                   help="大目录场景：漏登条目更新时间的偏移（默认 480）")
    p.add_argument("--missed-seen-offset", type=int, default=620,
                   help="大目录场景：漏登条目上目录的偏移（默认 620）")
    p.add_argument("--history-base", type=int, default=0,
                   help="大目录场景：历史卷宗最新一卷的偏移（默认 0）")
    p.add_argument("--history-spacing", type=int, default=60,
                   help="大目录场景：相邻历史卷宗更新时间的间隔（默认 60 秒）")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    log = logging.getLogger("mocksite")
    scenario_kwargs = {
        "history_base": args.history_base,
        "history_spacing": args.history_spacing,
        "last_scan_offset": args.last_scan_offset,
        "missed_offset": args.missed_offset,
        "missed_seen_offset": args.missed_seen_offset,
    }
    server = build_server(args.host, args.port, args.rate,
                          history_pages=args.history_pages,
                          start_site_time=args.start_site_time,
                          catalog_delay=args.catalog_delay,
                          scenario_kwargs=scenario_kwargs)
    log.info("模拟站启动: http://%s:%d （虚拟时钟 %d 倍速）",
             args.host, args.port, args.rate)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("模拟站停止")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
