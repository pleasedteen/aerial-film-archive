"""模拟站 HTTP 服务：分页目录、卷宗详情、支持 Range 的影像下载。

除了原有的路由与故障注入，还提供 ``ServerControl`` 供验证脚本做“停采”
实验：
- 记录每个请求的到达时刻（用于断言停采后不再有新请求）；
- 可挂起目录响应（客户端阻塞在等响应头）；
- 可挂起影像响应体（发完响应头后分两段写，客户端阻塞在读 body）；
- 可强制返回一次 503 + 长 Retry-After（客户端阻塞在统一暂停里）。

每个 server 实例使用自己的 Handler 子类，因此同一进程内可以同时起多个
互不干扰的模拟站（用于验证“另一站采集不受影响”）。
"""
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


class ServerControl:
    """验证用的请求观测与阻塞控制。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._arrivals: list[tuple[float, str]] = []
        # 目录响应门：默认放行；hold 后服务端在发响应前阻塞
        self._catalog_gate = threading.Event()
        self._catalog_gate.set()
        # 影像响应体门：默认放行
        self._image_gate = threading.Event()
        self._image_gate.set()
        self._image_hold: tuple[int, int] | None = None  # (nth, after_bytes)
        self._image_seen = 0
        self.image_hold_claimed = threading.Event()
        # 强制状态响应
        self._forced: list[dict] = []

    # -- 请求记录 -------------------------------------------------------- #
    def record_arrival(self, path: str) -> None:
        with self._lock:
            self._arrivals.append((time.monotonic(), path))

    def request_count(self, prefix: str = "") -> int:
        with self._lock:
            return sum(1 for _, p in self._arrivals if p.startswith(prefix))

    # -- 目录响应挂起 ---------------------------------------------------- #
    def hold_catalog(self) -> None:
        self._catalog_gate.clear()

    def release_catalog(self) -> None:
        self._catalog_gate.set()

    def wait_catalog(self, timeout: float = 30.0) -> None:
        self._catalog_gate.wait(timeout)

    # -- 影像响应体挂起 -------------------------------------------------- #
    def arm_image_body_hold(self, after_bytes: int = 4096, nth: int = 1) -> None:
        """让第 nth 个数据响应先写 after_bytes 再挂起，直到 release。"""
        with self._lock:
            self._image_hold = (nth, after_bytes)
            self._image_seen = 0
        self.image_hold_claimed.clear()
        self._image_gate.clear()

    def release_image_body(self) -> None:
        self._image_gate.set()

    def claim_image_body(self, total: int):
        """若本次数据响应应被挂起，返回 (after_bytes, gate)。

        总长度不超过 after_bytes 的小响应（如 Range 探测 bytes=0-0）不计数、
        不挂起，保证挂起落在真正的数据段上。
        """
        with self._lock:
            if self._image_hold is None:
                return None
            nth, after = self._image_hold
            if after >= total:
                return None
            self._image_seen += 1
            if self._image_seen != nth:
                return None
            self._image_hold = None
            self.image_hold_claimed.set()
            return after, self._image_gate

    # -- 强制 503 -------------------------------------------------------- #
    def force_status(self, prefix: str, status: int, retry_after: float,
                     times: int = 1) -> None:
        with self._lock:
            self._forced.append({
                "prefix": prefix, "status": status,
                "retry_after": retry_after, "remaining": times,
            })

    def take_forced(self, path: str):
        with self._lock:
            for item in self._forced:
                if item["remaining"] > 0 and path.startswith(item["prefix"]):
                    item["remaining"] -= 1
                    return item["status"], item["retry_after"]
        return None

    # -- 收尾 ------------------------------------------------------------ #
    def release_all(self) -> None:
        self._catalog_gate.set()
        self._image_gate.set()


class MockHandler(BaseHTTPRequestHandler):
    server_version = "MockArchive/1.0"

    # 由 build_server 注入（每个 server 用独立子类，互不干扰）
    scenario: Scenario
    rate_window: _RateWindow
    control: ServerControl
    real_start: float

    # ------------------------------------------------------------------ #
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        log.info("%s - %s", self.address_string(), fmt % args)

    def _site_time(self) -> int:
        return self.scenario.site_time(time.monotonic(), self.real_start)

    def _send(self, status: int, body: bytes = b"",
              content_type: str = "text/html; charset=utf-8",
              extra_headers: dict[str, str] | None = None,
              body_gate=None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Site-Time", str(self._site_time()))
        self.send_header("Accept-Ranges", "bytes")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command == "HEAD":
            return
        if body_gate is not None:
            after, gate = body_gate
            try:
                self.wfile.write(body[:after])
                self.wfile.flush()
            except OSError:
                return
            gate.wait(timeout=30)
            try:
                self.wfile.write(body[after:])
                self.wfile.flush()
            except OSError:
                # 客户端停采已关闭连接
                pass
            return
        try:
            self.wfile.write(body)
        except OSError:
            pass

    # ------------------------------------------------------------------ #
    def do_GET(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        path = parts.path

        if path != "/healthz":
            self.control.record_arrival(path)

        # 强制的 503（站点统一暂停），优先于限流窗口，保证可回放
        forced = self.control.take_forced(path) if path != "/healthz" else None
        if forced is not None:
            status, retry_after = forced
            self._send(
                status, b"<html><body>slow down</body></html>",
                extra_headers={"Retry-After": str(retry_after)},
            )
            log.warning("强制返回 %s Retry-After=%s（%s）", status, retry_after, path)
            return

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
            # 验证“等待目录响应时停采”：发响应前阻塞
            self.control.wait_catalog()
            self._handle_catalog(parse_qs(parts.query))
        elif path.startswith("/volumes/"):
            self._handle_detail(path.rsplit("/", 1)[-1])
        elif path.startswith("/images/"):
            self._handle_image(path.rsplit("/", 1)[-1])
        else:
            self._send(404, b"<html><body>not found</body></html>")

    do_HEAD = do_GET

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
            plan = self.control.claim_image_body(total)
            self._send(200, data, "application/octet-stream", body_gate=plan)
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
        plan = self.control.claim_image_body(len(body))
        self._send(
            206,
            body,
            "application/octet-stream",
            extra_headers={
                "Content-Range": f"bytes {start}-{end}/{total}",
            },
            body_gate=plan,
        )


def build_server(host: str, port: int, rate: int) -> ThreadingHTTPServer:
    scenario = Scenario(rate=rate)
    rate_window = _RateWindow(RATE_LIMIT_PER_SECOND)
    control = ServerControl()
    real_start = time.monotonic()

    # 每个 server 使用自己的 Handler 子类，避免多站互相覆盖类属性
    handler = type(
        "MockHandler_" + str(id(scenario)),
        (MockHandler,),
        {
            "scenario": scenario,
            "rate_window": rate_window,
            "control": control,
            "real_start": real_start,
        },
    )

    server = ThreadingHTTPServer((host, port), handler)
    server.scenario = scenario            # type: ignore[attr-defined]
    server.rate_window = rate_window      # type: ignore[attr-defined]
    server.control = control              # type: ignore[attr-defined]
    server.real_start = real_start        # type: ignore[attr-defined]
    server.daemon_threads = True
    return server

