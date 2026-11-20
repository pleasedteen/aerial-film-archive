"""模拟站 HTTP 服务：分页目录、卷宗详情、支持 Range 的影像下载。"""
from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .scenario import PAGE_SIZE, Scenario

log = logging.getLogger("mocksite")

# 滚动 1 秒窗口内允许的请求数，超出即 503 + Retry-After
RATE_LIMIT_PER_SECOND = 14
RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")


class _RateWindow:
    """1 秒滚动窗口限流（线程安全）。"""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._stamps: deque[float] = deque()
        self._lock = threading.Lock()
        self.rejected = 0

    def allow(self) -> bool:
        now = time.monotonic()
        with self._lock:
            while self._stamps and now - self._stamps[0] >= 1.0:
                self._stamps.popleft()
            if len(self._stamps) >= self.limit:
                self.rejected += 1
                return False
            self._stamps.append(now)
            return True


class ReplayPlan:
    """回放端点的可配置故障计划（线程安全）。

    - status 计划：前 count 次命中该端点返回指定状态码（带 Retry-After），
      之后恢复 200；
    - 延迟计划：每次命中都按固定秒数制造慢首字节 / 慢响应体，
      用于验证“慢建连/慢响应体不挡其他已就绪请求”。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # endpoint -> [剩余次数, 状态码, retry_after]
        self.status_plans: dict[str, list] = {}
        # endpoint -> 慢首字节秒数
        self.slow_head: dict[str, float] = {}
        # endpoint -> 慢响应体秒数
        self.slow_body: dict[str, float] = {}

    def set_status(self, endpoint: str, count: int,
                   status: int, retry_after: float = 1.0) -> None:
        with self._lock:
            self.status_plans[endpoint] = [count, status, retry_after]

    def set_slow_head(self, endpoint: str, seconds: float) -> None:
        with self._lock:
            self.slow_head[endpoint] = seconds

    def set_slow_body(self, endpoint: str, seconds: float) -> None:
        with self._lock:
            self.slow_body[endpoint] = seconds

    def take_status(self, endpoint: str) -> tuple[int, float] | None:
        with self._lock:
            plan = self.status_plans.get(endpoint)
            if not plan or plan[0] <= 0:
                return None
            plan[0] -= 1
            return plan[1], plan[2]

    def get_slow_head(self, endpoint: str) -> float:
        with self._lock:
            return self.slow_head.get(endpoint, 0.0)

    def get_slow_body(self, endpoint: str) -> float:
        with self._lock:
            return self.slow_body.get(endpoint, 0.0)


class MockHandler(BaseHTTPRequestHandler):
    server_version = "MockArchive/1.0"

    # 由 server 实例注入
    scenario: Scenario
    rate_window: _RateWindow
    real_start: float

    # ------------------------------------------------------------------ #
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        log.info("%s - %s", self.address_string(), fmt % args)

    def _site_time(self) -> int:
        return self.scenario.site_time(time.monotonic(), self.real_start)

    def _send(self, status: int, body: bytes = b"",
              content_type: str = "text/html; charset=utf-8",
              extra_headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Site-Time", str(self._site_time()))
        self.send_header("Accept-Ranges", "bytes")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    # ------------------------------------------------------------------ #
    def do_GET(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        path = parts.path

        # 全局限流（健康检查除外）
        if path != "/healthz" and not self.rate_window.allow():
            self._send(
                503,
                b"<html><body>service busy</body></html>",
                extra_headers={"Retry-After": "1"},
            )
            log.warning("触发限流，返回 503 Retry-After=1")
            return

        if path == "/healthz":
            self._send(200, b"ok", "text/plain")
        elif path == "/catalog":
            self._handle_catalog(parse_qs(parts.query))
        elif path.startswith("/volumes/"):
            self._handle_detail(path.rsplit("/", 1)[-1])
        elif path.startswith("/images/"):
            self._handle_image(path.rsplit("/", 1)[-1])
        elif path.startswith("/_replay/"):
            self._handle_replay(path.rsplit("/", 1)[-1])
        else:
            self._send(404, b"<html><body>not found</body></html>")

    do_HEAD = do_GET

    # ------------------------------------------------------------------ #
    def _handle_replay(self, endpoint: str) -> None:
        """回放专用端点：慢首字节 / 慢响应体 / 429 / 恒定 503。

        路由（均计入滚动窗口限流，健康检查之外一视同仁）：
          /_replay/slow-head   配置后延迟回响应头（模拟慢建连后慢首字节）
          /_replay/slow-body   响应头秒回，响应体按配置放慢写出（模拟慢响应体）
          /_replay/rate429     按计划返回 429 + Retry-After，耗尽后 200
          /_replay/busy503     按计划返回 503 + Retry-After，耗尽后 200
          /_replay/ok          始终秒回 200
        计划通过 ReplayPlan 注入（见 build_server / replay 验证脚本）。
        """
        plan: ReplayPlan = self.replay_plan
        hit = plan.take_status(endpoint)
        if hit is not None:
            status, retry_after = hit
            self._send(
                status,
                b"<html><body>replay fault</body></html>",
                extra_headers={"Retry-After": str(retry_after)},
            )
            log.info("回放端点 %s 注入故障 %s Retry-After=%s",
                     endpoint, status, retry_after)
            return

        slow_head = plan.get_slow_head(endpoint)
        if slow_head > 0:
            time.sleep(slow_head)  # 线程化服务：只拖慢当前连接

        slow_body = plan.get_slow_body(endpoint)
        body = b"".join(bytes([i % 256]) for i in range(4096))
        if slow_body <= 0:
            self._send(200, body, "application/octet-stream")
            return

        # 慢响应体：响应头立即发出，body 分块睡眠写出
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Site-Time", str(self._site_time()))
        self.end_headers()
        block = 256
        for start in range(0, len(body), block):
            self.wfile.write(body[start:start + block])
            self.wfile.flush()
            time.sleep(slow_body)

    # ------------------------------------------------------------------ #
    def _handle_catalog(self, query: dict[str, list[str]]) -> None:
        site_time = self._site_time()
        entries = self.scenario.catalog_entries(site_time)

        # 目录会重复出现链接：V0003 每次出现都给两条链接
        decorated: list = []
        for vol in entries:
            decorated.append(vol)
            if vol.volume_id == "V0003":
                decorated.append(vol)

        try:
            page = max(1, int(query.get("page", ["1"])[0]))
        except ValueError:
            page = 1
        start = (page - 1) * PAGE_SIZE
        page_items = decorated[start:start + PAGE_SIZE]
        total_pages = max(1, (len(decorated) + PAGE_SIZE - 1) // PAGE_SIZE)

        rows = []
        for vol in page_items:
            ver = vol.version_at(site_time)
            rows.append(
                f'<li><a class="entry-link" href="/volumes/{vol.volume_id}" '
                f'data-volume-id="{vol.volume_id}" '
                f'data-updated-at="{ver.updated_at}">'
                f"卷宗 {vol.volume_id}（版本 v{ver.version}，"
                f"更新时间 {ver.updated_at}）</a></li>"
            )
        next_link = ""
        if page < total_pages:
            next_link = f'<a class="next-page" href="/catalog?page={page + 1}">下一页</a>'
        html = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>目录第 {page} 页</title></head><body>"
            f"<h1>航拍底片目录（站点时间 {site_time}，第 {page}/{total_pages} 页）</h1>"
            f"<ul>{''.join(rows)}</ul>{next_link}</body></html>"
        )
        self._send(200, html.encode("utf-8"))

    # ------------------------------------------------------------------ #
    def _handle_detail(self, volume_id: str) -> None:
        site_time = self._site_time()
        vol = self.scenario.volumes.get(volume_id)
        # 补登：条目在 first_seen_at 之前查详情返回 404
        if vol is None or not vol.is_visible(site_time):
            self._send(404, b"<html><body>volume not listed yet</body></html>")
            return
        ver = vol.version_at(site_time)
        html = f"""<!doctype html><html><head><meta charset='utf-8'>
<title>卷宗 {volume_id}</title></head><body>
<article class="volume-detail"
 data-volume-id="{vol.volume_id}"
 data-updated-at="{ver.updated_at}"
 data-checksum="{ver.checksum}"
 data-size="{ver.size}"
 data-version="{ver.version}">
<h1>卷宗 {vol.volume_id}</h1>
<dl>
<dt>卷宗编号</dt><dd class="field-volume-id">{vol.volume_id}</dd>
<dt>版本</dt><dd class="field-version">{ver.version}</dd>
<dt>版本更新时间（按秒）</dt><dd class="field-updated-at">{ver.updated_at}</dd>
<dt>内容校验值（sha256）</dt><dd class="field-checksum">{ver.checksum}</dd>
<dt>影像字节数</dt><dd class="field-size">{ver.size}</dd>
</dl>
<p>下载（换版后地址沿用）：
<a class="image-link" href="/images/{vol.volume_id}">{vol.volume_id}.img</a></p>
</article></body></html>"""
        self._send(200, html.encode("utf-8"))

    # ------------------------------------------------------------------ #
    def _handle_image(self, volume_id: str) -> None:
        site_time = self._site_time()
        vol = self.scenario.volumes.get(volume_id)
        if vol is None or not vol.is_visible(site_time):
            self._send(404, b"<html><body>image not found</body></html>")
            return
        ver = vol.version_at(site_time)

        # 该版本影像尚未开放（补登窗口内的迟到影像）
        if ver.visible_at is not None and site_time < ver.visible_at:
            self._send(404, b"<html><body>image not available yet</body></html>")
            return

        # 故障注入（一次性，按请求序号）
        failure = self.scenario.consume_failure(
            volume_id, ver.version, vol.image_fail_sequence
        )
        if failure is not None:
            if failure == 503:
                self._send(
                    503, b"<html><body>slow down</body></html>",
                    extra_headers={"Retry-After": "1"},
                )
            else:
                self._send(failure, b"<html><body>temporary fault</body></html>")
            log.warning("卷宗 %s v%s 注入故障 %s", volume_id, ver.version, failure)
            return

        data = self.scenario.images[(volume_id, ver.version)]
        total = len(data)
        range_header = self.headers.get("Range")
        if not range_header:
            self._send(200, data, "application/octet-stream")
            return

        match = RANGE_RE.fullmatch(range_header.strip())
        if not match:
            self._send(416, b"", extra_headers={"Content-Range": f"bytes */{total}"})
            return
        start_s, end_s = match.groups()
        if start_s == "" and end_s:
            # 后缀形式 bytes=-N：最后 N 字节
            suffix = int(end_s)
            start, end = max(0, total - suffix), total - 1
        else:
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else total - 1
        if start >= total or start > end:
            self._send(416, b"", extra_headers={"Content-Range": f"bytes */{total}"})
            return
        end = min(end, total - 1)
        body = data[start:end + 1]
        self._send(
            206,
            body,
            "application/octet-stream",
            extra_headers={
                "Content-Range": f"bytes {start}-{end}/{total}",
            },
        )


def build_server(host: str, port: int, rate: int,
                 replay_plan: ReplayPlan | None = None) -> ThreadingHTTPServer:
    scenario = Scenario(rate=rate)
    rate_window = _RateWindow(RATE_LIMIT_PER_SECOND)
    real_start = time.monotonic()

    handler = MockHandler
    handler.scenario = scenario
    handler.rate_window = rate_window
    handler.real_start = real_start
    handler.replay_plan = replay_plan or ReplayPlan()

    server = ThreadingHTTPServer((host, port), handler)
    server.scenario = scenario            # type: ignore[attr-defined]
    server.rate_window = rate_window      # type: ignore[attr-defined]
    server.replay_plan = handler.replay_plan  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server
