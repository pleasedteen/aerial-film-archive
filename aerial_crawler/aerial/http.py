"""HTTP 客户端：按请求头实际发出时刻限流 + 退避重试 + 分段读取。

仅依赖标准库 http.client。每次请求（含重试）都经历：

    limiter.reserve()  →  connect()（慢建连，不占队首）
    → limiter.wait_turn()（最小间隔 / 统一暂停放行）
    → endheaders()（请求头实际发出，记录 t_sent）
    → getresponse()/read()（慢响应体，票号已离队，不挡其他请求）

收到 503/429 + Retry-After 时，无论该请求是否已用完重试次数，都会先通过
限流器统一暂停全部 worker 的新请求，再决定重试或抛出。
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
from .limiter import RateLimiter, Ticket
from .request_log import RequestLog, classify_target

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
    def __init__(self, config: Config, limiter: RateLimiter,
                 request_log: Optional[RequestLog] = None) -> None:
        self.cfg = config
        self.limiter = limiter
        self.log = request_log
        parts = urlsplit(config.base_url)
        if parts.scheme not in ("http", "https"):
            raise ValueError("base_url 必须是 http(s)")
        self._scheme = parts.scheme
        self._host = parts.hostname or "127.0.0.1"
        self._port = parts.port or (443 if parts.scheme == "https" else 80)
        self._prefix = parts.path.rstrip("/")
        self.retries = 0

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

    def _new_connection(self) -> http.client.HTTPConnection:
        # 建连阶段用 connect_timeout；连上后切换为 read_timeout
        if self._scheme == "https":
            return http.client.HTTPSConnection(
                self._host, self._port, timeout=self.cfg.connect_timeout,
            )
        return http.client.HTTPConnection(
            self._host, self._port, timeout=self.cfg.connect_timeout,
        )

    def _one_attempt(self, target: str, headers: dict[str, str],
                     attempt: int, external_ticket: Optional[Ticket] = None
                     ) -> "tuple[Response, dict]":
        """完整执行一次尝试，返回响应与计时记录。任何失败都向上抛。

        external_ticket 用于回放验证：调用方预先取票以固定 FIFO 位置，
        本方法直接在该票号上等待放行，不再自行 reserve。
        """
        kind = classify_target(target)
        conn = self._new_connection()
        ticket: Ticket = external_ticket or self.limiter.reserve()
        rec: dict = {
            "kind": kind,
            "target": target,
            "attempt": attempt,
            "status": None,
            "error": None,
            "retry_after": None,
            "paused": False,
            "t_reserve": None,
            "t_connected": None,
            "t_sent": None,
            "t_response_head": None,
            "t_done": None,
            "connect_ms": None,
            "gate_wait_ms": None,
            "ttfb_ms": None,
            "body_ms": None,
        }
        t_reserve = self.log.now() if self.log else time.monotonic()
        rec["t_reserve"] = self._rel(t_reserve)
        try:
            # 1) 慢建连发生在限流放行之前，不占用队首、不挡其他就绪请求
            conn.connect()
            t_connected = time.monotonic()
            rec["t_connected"] = self._rel(t_connected)
            rec["connect_ms"] = round((t_connected - t_reserve) * 1000, 3)

            # 2) 阻塞到本票可以发出请求头（最小间隔 + 统一暂停 + FIFO）
            t_gate_start = time.monotonic()
            self.limiter.wait_turn(ticket)
            rec["paused"] = ticket.paused
            rec["gate_wait_ms"] = round((time.monotonic() - t_gate_start) * 1000, 3)

            # 3) 请求头实际发出：endheaders 返回即请求头已写入内核，
            # 紧随其后采样的 t_sent 就是限流口径的“实际发出时刻”
            conn.sock.settimeout(self.cfg.read_timeout)
            conn.putrequest("GET", target, skip_accept_encoding=True)
            for k, v in headers.items():
                conn.putheader(k, v)
            conn.endheaders()
            t_sent = time.monotonic()
            rec["t_sent"] = self._rel(t_sent)

            # 4) 慢响应体在这里发生；票号已出队，其他 worker 不受影响
            resp = conn.getresponse()
            t_head = time.monotonic()
            rec["t_response_head"] = self._rel(t_head)
            rec["ttfb_ms"] = round((t_head - t_sent) * 1000, 3)
            data = resp.read()
            t_done = time.monotonic()
            rec["t_done"] = self._rel(t_done)
            rec["body_ms"] = round((t_done - t_head) * 1000, 3)

            raw = {k.lower(): v for k, v in resp.getheaders()}
            rec["status"] = resp.status
            if "retry-after" in raw:
                rec["retry_after"] = raw["retry-after"]
            site_time_raw = raw.get("x-site-time")
            site_time = int(site_time_raw) \
                if site_time_raw and site_time_raw.isdigit() else None
            return Response(
                status=resp.status, data=data, headers=raw,
                site_time=site_time, url=target,
            ), rec
        except (OSError, http.client.HTTPException) as exc:
            rec["error"] = f"{type(exc).__name__}: {exc}"
            rec["t_done"] = self._rel(time.monotonic())
            raise
        finally:
            self.limiter.release(ticket)
            conn.close()
            if self.log is not None:
                self.log.append(rec)

    def _rel(self, mono: float) -> float:
        return self.log.relative(mono) if self.log else round(mono, 6)

    def get(self, url: str, *, extra_headers: Optional[dict[str, str]] = None) -> Response:
        """带限流、退避重试、503/429 统一暂停的 GET。

        - 2xx 直接返回；
        - 503/429：解析 Retry-After，调用 limiter.pause 统一推迟全部新请求
          （即使本次请求已用完重试次数，暂停也照样生效），随后重试或抛出；
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
            try:
                resp, _rec = self._one_attempt(target, headers, attempt)
            except (OSError, http.client.HTTPException) as exc:
                if attempt >= self.cfg.max_retries:
                    raise TransientHTTPError(
                        f"连接失败且重试耗尽: {target}: {exc}") from exc
                self._sleep_backoff(attempt, f"连接异常 {exc}")
                attempt += 1
                continue

            if resp.status in (503, 429):
                retry_after = self._parse_retry_after(
                    resp.headers.get("retry-after"))
                # 先统一暂停所有新请求——无论本次请求是否还有重试额度
                self.limiter.pause(
                    retry_after,
                    reason=f"HTTP {resp.status} {target}（第 {attempt + 1} 次尝试）",
                )
                if attempt >= self.cfg.max_retries:
                    raise TransientHTTPError(
                        f"{resp.status} 且重试耗尽: {target}（暂停已生效）")
                attempt += 1
                self.retries += 1
                continue

            if 500 <= resp.status < 600:
                if attempt >= self.cfg.max_retries:
                    raise TransientHTTPError(
                        f"HTTP {resp.status} 且重试耗尽: {target}")
                self._sleep_backoff(attempt, f"HTTP {resp.status}")
                attempt += 1
                self.retries += 1
                continue

            if resp.status >= 400:
                raise PermanentHTTPError(resp.status, target)

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
    def get_range(self, url: str, start: int, end_inclusive: int) -> Response:
        """读取闭区间 [start, end_inclusive] 的字节。

        服务端正常返回 206；若忽略 Range 返回 200，调用方按完整响应自行处理。
        """
        headers = {"Range": f"bytes={start}-{end_inclusive}"}
        return self.get(url, extra_headers=headers)

    def head_size(self, url: str) -> int:
        """通过一次 Range 请求探测影像总长度（Content-Range: bytes */total）。"""
        resp = self.get_range(url, 0, 0)
        if resp.status == 206 and "content-range" in resp.headers:
            total = resp.headers["content-range"].split("/")[-1]
            if total.isdigit():
                return int(total)
        if "content-length" in resp.headers and \
                resp.headers["content-length"].isdigit():
            # 服务端把 0-0 当成普通 GET 返回了整段
            return int(resp.headers["content-length"])
        raise TransientHTTPError(f"无法从响应推断影像大小: {url}")
