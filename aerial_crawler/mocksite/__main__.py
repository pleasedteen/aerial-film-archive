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
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    log = logging.getLogger("mocksite")
    server = build_server(args.host, args.port, args.rate)
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
