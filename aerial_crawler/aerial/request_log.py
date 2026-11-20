"""逐条请求的可核对记录（请求头实际发出时刻 + 各阶段耗时）。

每条请求（目录 / 详情 / 影像区段 / 重试，一视同仁）在结束时追加一行 JSON：

    {"seq": 12, "kind": "image", "target": "/images/V0001",
     "attempt": 2, "status": 206,
     "t_reserve": 0.0012, "t_connected": 0.0034, "t_sent": 0.0501,
     "t_response_head": 0.0610, "t_done": 0.0823,
     "connect_ms": 2.2, "ttfb_ms": 10.9, "body_ms": 21.3,
     "retry_after": null, "paused": false, "error": null}

时间戳 t_* 均为相对记录器创建时刻的真实秒数；t_sent 即请求头实际发出
时刻，最小间隔与统一暂停的核对都以它为准。每行一次 fsync，可随时 tail。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional


def classify_target(target: str) -> str:
    """按请求路径归类：catalog / detail / image / other。"""
    path = target.split("?", 1)[0]
    if path.startswith("/catalog"):
        return "catalog"
    if path.startswith("/volumes/"):
        return "detail"
    if path.startswith("/images/"):
        return "image"
    return "other"


class RequestLog:
    def __init__(self, path: Optional[Path]) -> None:
        import time
        self.path = path
        self._lock = threading.Lock()
        # 创建即固定基准，保证所有相对时间戳 >= 0
        self._t0: float = time.monotonic()
        self._seq = 0
        self.records: list[dict] = []
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    def now(self) -> float:
        import time
        return time.monotonic()

    def relative(self, mono: float) -> float:
        return round(mono - self._t0, 6)

    def append(self, record: dict) -> dict:
        with self._lock:
            self._seq += 1
            record = {"seq": self._seq, **record}
            self.records.append(record)
            if self.path is not None:
                line = json.dumps(record, ensure_ascii=False, sort_keys=True)
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
            return record
