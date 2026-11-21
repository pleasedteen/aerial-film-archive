"""HTTP 客户端：全局限流 + 临时故障退避重试 + 分段读取。

仅依赖标准库 http.client。每个请求都经过同一个 RateLimiter；
收到 503/429 + Retry-After 时通过限流器统一暂停全部 worker。
"""
from __future__ import annotations

import http.client
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit, urljoin

from .config import Config
from .limiter import RateLimiter

log = logging.getLogger(__name__)


class TransientHTTPError(Exception):
    """可重试的临时故障（5xx、连接错误、503/429）。"""


class PermanentHTTPError(Exception):
    """不重试的响应（如 404）。status 为 HTTP 状态码。"""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"HTTP {status}: {url}")
        self.status = status
        self.url = url


@dataclass
class Response:
    status: int
    data: bytes
    headers: dict[str, str]
    site_time: Optional[int]
    url: str


class Client:
    def __init__(self, config: Config, limiter: RateLimiter) -> None:
        self.cfg = config
        self.limiter = limiter
        parts = urlsplit(config.base_url)
        if parts.scheme not in ("http", "https"):
            raise ValueError("base_url 必须是 http(s)")
        self._scheme = parts.scheme
        self._host = parts.hostname or "127.0.0.1"
        self._port = parts.port or (443 if parts.scheme == "https" else 80)
        self._prefix = parts.path.rstrip("/")
        self.retries = 0
        # 每次底层尝试（含被重试的 5xx/503）后的诊断回调，签名：
        # (req_id, target, attempt_no, status|None, error|None, site_time|None)
        # req_id 由调用方显式传入，避免多线程下线程本地变量串台；
        # 未传时为 None（无诊断上下文的普通调用）。
        self.attempt_hook = None

    def _notify_attempt(self, target: str, attempt: int,
                        status: Optional[int], error: Optional[str],
                        site_time: Optional[int] = None,
                        req_id: Optional[str] = None) -> None:
        if self.attempt_hook is not None:
            try:
                self.attempt_hook(req_id, target, attempt, status, error, site_time)
            except Exception:  # noqa: BLE001 - 诊断不能影响采集主流程
                log.debug("诊断尝试回调异常", exc_info=True)

    # ------------------------------------------------------------------ #
    # 内部原语
    # ------------------------------------------------------------------ #
    def absolute(self, url: str) -> str:
        return urljoin(self.cfg.base_url, url)

    def _target(self, url: str) -> str:
        """把（可能是相对的）url 转成主机之后的 path+query。"""
        if url.startswith("http://") or url.startswith("https://"):
            p = urlsplit(url)
            if p.hostname != self._host or p.port not in (None, self._port):
                raise ValueError(f"拒绝跨主机 URL: {url}")
            target = p.path or "/"
            if p.query:
                target += "?" + p.query
            return target
        if not url.startswith("/"):
            url = self._prefix + "/" + url
        return url

    def _one_request(self, target: str, headers: dict[str, str]) -> Response:
        """真正发起一次请求（调用方负责限流与重试）。"""
        conn: http.client.HTTPConnection
        if self._scheme == "https":
            conn = http.client.HTTPSConnection(
                self._host, self._port,
                timeout=self.cfg.read_timeout,
            )
        else:
            conn = http.client.HTTPConnection(
                self._host, self._port,
                timeout=self.cfg.read_timeout,
            )
        try:
            conn.request("GET", target, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            raw = {k.lower(): v for k, v in resp.getheaders()}
            status = resp.status
        finally:
            conn.close()

        site_time_raw = raw.get("x-site-time")
        site_time = int(site_time_raw) if site_time_raw and site_time_raw.isdigit() else None
        return Response(
            status=status,
            data=data,
            headers=raw,
            site_time=site_time,
            url=target,
        )

    def get(self, url: str, *, extra_headers: Optional[dict[str, str]] = None,
            req_id: Optional[str] = None) -> Response:
        """带限流、退避重试、503 统一暂停的 GET。

        - 2xx 直接返回；
        - 503/429：解析 Retry-After，调用 limiter.pause 统一推迟后重试；
        - 5xx / 连接错误：指数退避 + 抖动重试；
        - 404：抛 PermanentHTTPError（补登窗口内由上层当作“稍后再试”处理）；
        - 其他 4xx：抛 PermanentHTTPError。
        """
        target = self._target(url)
        headers = {
            "Host": self._host,
            "User-Agent": self.cfg.user_agent,
            "Accept": "*/*",
        }
        if extra_headers:
            headers.update(extra_headers)

        attempt = 0
        while True:
            self.limiter.acquire()
            try:
                resp = self._one_request(target, headers)
            except (OSError, http.client.HTTPException) as exc:
                self._notify_attempt(target, attempt + 1, None, f"{type(exc).__name__}: {exc}", req_id=req_id)
                if attempt >= self.cfg.max_retries:
                    raise TransientHTTPError(f"连接失败且重试耗尽: {target}: {exc}") from exc
                self._sleep_backoff(attempt, f"连接异常 {exc}")
                attempt += 1
                continue

            if resp.status in (503, 429):
                self._notify_attempt(target, attempt + 1, resp.status, None,
                                     resp.site_time, req_id=req_id)
                if attempt >= self.cfg.max_retries:
                    raise TransientHTTPError(f"{resp.status} 且重试耗尽: {target}")
                retry_after = self._parse_retry_after(resp.headers.get("retry-after"))
                # 站点要求暂停：统一推迟所有新请求
                self.limiter.pause(retry_after, reason=f"HTTP {resp.status} {target}")
                attempt += 1
                self.retries += 1
                continue

            if 500 <= resp.status < 600:
                self._notify_attempt(target, attempt + 1, resp.status, None,
                                     resp.site_time, req_id=req_id)
                if attempt >= self.cfg.max_retries:
                    raise TransientHTTPError(f"HTTP {resp.status} 且重试耗尽: {target}")
                self._sleep_backoff(attempt, f"HTTP {resp.status}")
                attempt += 1
                self.retries += 1
                continue

            if resp.status >= 400:
                self._notify_attempt(target, attempt + 1, resp.status, None,
                                     resp.site_time, req_id=req_id)
                raise PermanentHTTPError(resp.status, target)

            self._notify_attempt(target, attempt + 1, resp.status, None,
                                 resp.site_time, req_id=req_id)
            return resp

    def _sleep_backoff(self, attempt: int, reason: str) -> None:
        delay = min(self.cfg.backoff_cap, self.cfg.backoff_base * (2 ** attempt))
        delay *= 0.5 + random.random()  # 抖动，避免并发 worker 同步重连
        self.retries += 1
        log.info("临时故障（%s），第 %d 次退避 %.2fs", reason, attempt + 1, delay)
        time.sleep(delay)

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> float:
        if not value:
            return 1.0
        value = value.strip()
        if value.isdigit():
            return max(0.0, float(value))
        # 支持 HTTP-date 形式（本项目模拟站始终发 delta-seconds）
        try:
            from email.utils import parsedate_to_datetime
            ts = parsedate_to_datetime(value).timestamp()
            return max(0.0, ts - time.time())
        except (TypeError, ValueError):
            return 1.0

    # ------------------------------------------------------------------ #
    # 分段读取
    # ------------------------------------------------------------------ #
    def get_range(self, url: str, start: int, end_inclusive: int,
                  req_id: Optional[str] = None) -> Response:
        """读取闭区间 [start, end_inclusive] 的字节。

        服务端正常返回 206；若忽略 Range 返回 200，调用方按完整响应自行处理。
        """
        headers = {"Range": f"bytes={start}-{end_inclusive}"}
        return self.get(url, extra_headers=headers, req_id=req_id)

    def head_size(self, url: str, req_id: Optional[str] = None) -> int:
        """通过一次 Range 请求探测影像总长度（Content-Range: bytes */total）。"""
        resp = self.get_range(url, 0, 0, req_id=req_id)
        if resp.status == 206 and "content-range" in resp.headers:
            total = resp.headers["content-range"].split("/")[-1]
            if total.isdigit():
                return int(total)
        if "content-length" in resp.headers and resp.headers["content-length"].isdigit():
            # 服务端把 0-0 当成普通 GET 返回了整段
            return int(resp.headers["content-length"])
        raise TransientHTTPError(f"无法从响应推断影像大小: {url}")
