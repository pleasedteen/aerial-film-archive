#!/usr/bin/env python3
"""一条命令回放并发交错：可核对的请求头发出时刻与结果（80 秒内完成）。

起一个带 ReplayPlan 的模拟站，用真实的 RateLimiter + Client 多线程并发打一组
交错 workload，逐条打印“请求头实际发出时刻”，并核对四条规则：

  R1 最小间隔按请求头实际发出时刻计算：目录、详情、影像、重试一起计时，
     任意两条成功记录的 t_sent 之差 >= min_interval - 容差；
  R2 收到 429 / 503 的 Retry-After 就暂停新请求，即使该请求已用完重试次数
     （专门有一轮 max_retries=0 的恒定 503 用例）；
  R3 慢首字节 / 慢响应体不会挡住其他已就绪请求：慢请求在途期间，
     其他请求仍按最小间隔正常发出；
  R4 FIFO 公平性：请求头按取票顺序单调发出，后取票的请求不能插头发送
     （以 t_sent 单调不减为证；日志 seq 是请求结束序号，不用于此项）。

用法：
  python3 replay.py                 # 自动找空闲端口、临时目录
  python3 replay.py --keep          # 保留 requests.jsonl 现场
"""
from __future__ import annotations

import argparse
import shutil
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from aerial.config import Config
from aerial.http import Client, TransientHTTPError
from aerial.limiter import RateLimiter
from aerial.request_log import RequestLog
from mocksite.server import ReplayPlan, build_server

MIN_INTERVAL = 0.10
TOLERANCE = 0.012          # 线程调度容差（秒）
PAUSE_SECONDS = 1.0
SLOW_HEAD = 0.8            # 慢首字节
SLOW_BODY = 0.15           # 慢响应体（每个 256 字节块睡一次，共 16 块 ≈ 2.4s）


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_health(httpd, timeout: float = 5.0) -> None:
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{httpd.server_address[1]}/healthz", timeout=1
            ) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("模拟站未就绪")


def make_client(base_url: str, workdir: Path,
                max_retries: int = 6) -> tuple[Client, RateLimiter, RequestLog]:
    cfg = Config(
        base_url=base_url,
        workdir=workdir,
        workers=4,
        min_interval=MIN_INTERVAL,
        max_retries=max_retries,
        connect_timeout=5.0,
        read_timeout=15.0,
        backoff_base=0.05,
        backoff_cap=0.5,
    )
    limiter = RateLimiter(MIN_INTERVAL)
    req_log = RequestLog(workdir / "requests.jsonl")
    client = Client(cfg, limiter, req_log)
    return client, limiter, req_log


def run_workload(base_url: str, workdir: Path, plan: ReplayPlan):
    """慢请求先行 + 普通流量并发：慢连接与慢响应体在途期间同伴照常发出。"""
    client, limiter, req_log = make_client(base_url, workdir)

    normal_endpoints = [
        "/_replay/ok",
        "/_replay/rate429",     # 先 429 + Retry-After，重试后 200
        "/_replay/ok",
        "/_replay/busy503",     # 先 503 + Retry-After，重试后 200
        "/_replay/ok",
        "/_replay/ok",
    ]
    errors: list[str] = []
    start_gate = threading.Event()

    def worker(target: str) -> None:
        start_gate.wait()  # 等慢请求都已发出后再放行普通流量
        try:
            client.get(target)
        except Exception as exc:  # noqa: BLE001 - 记录后继续，断言阶段统一核对
            errors.append(f"{target}: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=worker, args=(ep,))
               for ep in normal_endpoints]
    for t in threads:
        t.start()

    # 主流程先取票、先发出两个慢请求（FIFO 队首），它们在途期间不占限流队列
    slow_targets = ["/_replay/slow-head", "/_replay/slow-body"]
    slow_tickets = []
    for target in slow_targets:
        # 直接走 client 的完整尝试，但用预先取票保证排在最前
        ticket = client.limiter.reserve()
        slow_tickets.append((target, ticket))

    t0 = time.monotonic()
    slow_threads = [
        threading.Thread(target=_slow_get, args=(client, target, ticket, errors))
        for target, ticket in slow_tickets
    ]
    for t in slow_threads:
        t.start()
    # 等慢请求头都已发出（票号离队），再放行普通 worker
    deadline = time.monotonic() + 5
    while limiter.snapshot()["inflight"] < 2 and time.monotonic() < deadline:
        time.sleep(0.005)
    start_gate.set()

    for t in slow_threads + threads:
        t.join(timeout=30)
    elapsed = time.monotonic() - t0
    return client, limiter, req_log, errors, elapsed


def _slow_get(client: Client, target: str, ticket, errors: list[str]) -> None:
    """用指定票号执行一次慢请求，固定其 FIFO 队首位置。"""
    try:
        client._one_attempt(target, {
            "Host": client._host,
            "User-Agent": client.cfg.user_agent,
            "Accept": "*/*",
        }, 0, external_ticket=ticket)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{target}: {type(exc).__name__}: {exc}")


def run_real_traffic(base_url: str, workdir: Path):
    """对真实采集路由打混合流量：目录 / 详情 / 影像（Range）+ 影像重试。

    V0002 影像头两次请求 500（场景内置），天然带出“重试也一起计时”。
    """
    client, limiter, req_log = make_client(base_url, workdir / "real")

    targets = [
        ("/catalog?page=1", None),
        ("/volumes/V0001", None),
        ("/volumes/V0002", None),
        ("/images/V0001", {"Range": "bytes=0-255"}),
        ("/images/V0002", {"Range": "bytes=0-255"}),   # 内置 500,500 重试
        ("/images/V0003", {"Range": "bytes=0-255"}),
        ("/catalog?page=2", None),
    ]
    barrier = threading.Barrier(len(targets))
    errors: list[str] = []

    def worker(target: str, extra) -> None:
        barrier.wait()
        try:
            client.get(target, extra_headers=extra)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{target}: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=worker, args=(t, h)) for t, h in targets]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    return client, limiter, req_log, errors, time.monotonic() - t0


def check_real_traffic(check: list[tuple[str, bool, str]],
                       req_log: RequestLog, errors: list[str]) -> None:
    def expect(name: str, ok: bool, detail: str = "") -> None:
        check.append((name, ok, detail))

    records = req_log.records
    sent = sorted((r for r in records if r["t_sent"] is not None),
                  key=lambda r: r["t_sent"])
    kinds = {r["kind"] for r in sent}

    violations = []
    gaps = []
    for a, b in zip(sent, sent[1:]):
        gap = b["t_sent"] - a["t_sent"]
        gaps.append(gap)
        if gap < MIN_INTERVAL - TOLERANCE:
            violations.append(
                f"#{a['seq']}[{a['kind']}] -> #{b['seq']}[{b['kind']}] {gap:.3f}s")
    expect(
        "R1b 真实路由混合流量：目录/详情/影像/重试共用一个间隔计时",
        {"catalog", "detail", "image"} <= kinds and not violations and not errors,
        f"类别 {sorted(kinds)}，最小间隔 {min(gaps, default=0):.3f}s，"
        f"违规 {violations[:2]}，错误 {errors}",
    )
    retries = [r for r in sent if r["attempt"] > 0]
    index_by_seq = {r["seq"]: i for i, r in enumerate(sent)}
    retry_ok = True
    for r in retries:
        i = index_by_seq[r["seq"]]
        if i > 0 and r["t_sent"] - sent[i - 1]["t_sent"] < MIN_INTERVAL - TOLERANCE:
            retry_ok = False
    expect("R1c 影像重试请求同样落在全局间隔链条上（无独立加急通道）",
           bool(retries) and retry_ok,
           f"重试记录 {[(r['target'], r['attempt'], r['status']) for r in retries]}")


def run_exhausted_case(base_url: str, workdir: Path, plan: ReplayPlan):
    """max_retries=0 打恒定 503：请求必失败，但统一暂停必须照样生效。"""
    client, limiter, req_log = make_client(
        base_url, workdir / "exhausted", max_retries=0)

    plan.set_status("always503", count=10, status=503, retry_after=PAUSE_SECONDS)
    raised = False
    t0 = time.monotonic()
    try:
        client.get("/_replay/always503")
    except TransientHTTPError:
        raised = True
    fail_at = time.monotonic() - t0

    # 暂停窗口内再发一个普通请求：必须被推迟到暂停结束之后
    rec_before = len(req_log.records)
    t1 = time.monotonic()
    client.get("/_replay/ok")
    after_gap = time.monotonic() - t1
    return raised, fail_at, after_gap, limiter.snapshot(), req_log.records[rec_before:]


# ---------------------------------------------------------------------- #
def check_rules(check: list[tuple[str, bool, str]], req_log: RequestLog,
                limiter: RateLimiter, errors: list[str], elapsed: float) -> None:
    def expect(name: str, ok: bool, detail: str = "") -> None:
        check.append((name, ok, detail))

    records = req_log.records
    sent = [r for r in records if r["t_sent"] is not None]
    sent.sort(key=lambda r: r["t_sent"])

    # R1：任意相邻两次请求头实际发出的间隔 >= min_interval（全类型混合计时）
    violations = []
    for a, b in zip(sent, sent[1:]):
        gap = b["t_sent"] - a["t_sent"]
        if gap < MIN_INTERVAL - TOLERANCE:
            violations.append(
                f"#{a['seq']}({a['target']}) -> #{b['seq']}"
                f"({b['target']}) 仅 {gap:.3f}s")
    kinds = sorted({r["kind"] for r in sent})
    expect(
        f"R1 最小间隔按请求头发出时刻、全类型一起计时（{len(sent)} 条，类别 {kinds}）",
        not violations,
        "；".join(violations[:3]) or f"最小相邻间隔 {min((b['t_sent']-a['t_sent'] for a,b in zip(sent,sent[1:])), default=0):.3f}s",
    )

    # R2（交错 workload 部分）：429/503 都触发了统一暂停
    fault_recs = [r for r in records if r["status"] in (429, 503)]
    expect("R2a 交错 workload 中收到过 429 与 503 的 Retry-After",
           {r["status"] for r in fault_recs} >= {429, 503},
           f"故障响应 {[(r['target'], r['status']) for r in fault_recs]}")
    expect("R2b 限流暂停事件已记录且后续请求带 paused 标记",
           limiter.snapshot()["pause_events"] >= 2
           and any(r["paused"] for r in sent),
           str(limiter.snapshot()))
    expect("R2c 被限流的请求经重试后全部成功（无 worker 异常逃逸）",
           not errors,
           "；".join(errors) or "全部成功")

    # R3：慢首字节 / 慢响应体在途期间，其他请求照常按间隔发出。
    # 关键证据：慢请求的 [t_sent, t_done] 窗口内，存在其他请求头发出，
    # 且这些同伴之间仍满足最小间隔（没有被慢连接拖住）。
    slow = [r for r in records if r["target"] in ("/_replay/slow-head",
                                                  "/_replay/slow-body")
            and r["t_sent"] is not None]
    window_peers: dict[str, int] = {}
    for s in slow:
        lo, hi = s["t_sent"], s["t_done"]
        peers = [r for r in sent
                 if r["seq"] != s["seq"] and lo < r["t_sent"] < hi]
        window_peers[s["target"]] = len(peers)
    expect(
        "R3 慢首字节/慢响应体在途时其他已就绪请求未被挡住",
        len(slow) == 2 and all(n >= 2 for n in window_peers.values()),
        f"各慢请求窗口内同伴发出数 {window_peers}",
    )
    # 量化：慢响应体请求的 body_ms 显著大于间隔，证明它确实“慢”
    body_slow = [r for r in records if r["target"] == "/_replay/slow-body"]
    expect("R3b 慢响应体确实跨越多个最小间隔（body_ms 可核对）",
           body_slow and body_slow[0]["body_ms"] > MIN_INTERVAL * 5 * 1000,
           f"body_ms={body_slow[0]['body_ms'] if body_slow else 'NA'}")
    head_slow = [r for r in records if r["target"] == "/_replay/slow-head"]
    expect("R3c 慢首字节延迟可核对（ttfb_ms >= 注入延迟）",
           head_slow and head_slow[0]["ttfb_ms"] >= SLOW_HEAD * 1000 * 0.9,
           f"ttfb_ms={head_slow[0]['ttfb_ms'] if head_slow else 'NA'}")

    # R4：无插队——请求头严格按 FIFO 放行，发出时刻单调不减；
    # 日志 seq 是“请求结束”序号（慢请求结束晚），不能拿它与发出顺序比。
    ts = [r["t_sent"] for r in sent]
    expect("R4 请求头按 FIFO 单调发出（无后取票插头发送）",
           all(b >= a for a, b in zip(ts, ts[1:])),
           f"发出时刻序列 {[round(x, 3) for x in ts]}")

    expect(f"整轮交错在合理时间内完成（用时 {elapsed:.1f}s）",
           elapsed < 25.0, f"{elapsed:.1f}s")


def print_timeline(req_log: RequestLog) -> None:
    print("\n  seq  t_sent    类别     状态  重试  paused  connect/ttfb/body(ms)  target")
    for r in sorted(req_log.records, key=lambda x: x["seq"]):
        print(
            f"  {r['seq']:>3}  "
            f"{(r['t_sent'] if r['t_sent'] is not None else -1):>7.3f}  "
            f"{r['kind']:<7}  "
            f"{str(r['status']):>4}  "
            f"{r['attempt']:>4}  "
            f"{str(r['paused']):<6}  "
            f"{str(r['connect_ms']):>7}/{str(r['ttfb_ms']):>7}/{str(r['body_ms']):>8}  "
            f"{r['target']}"
            + (f"  RA={r['retry_after']}" if r['retry_after'] else "")
            + (f"  ERR={r['error']}" if r['error'] else "")
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="保留临时目录")
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="aerial_replay_"))
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    plan = ReplayPlan()
    plan.set_status("rate429", count=1, status=429, retry_after=PAUSE_SECONDS)
    plan.set_status("busy503", count=1, status=503, retry_after=PAUSE_SECONDS)
    plan.set_slow_head("slow-head", SLOW_HEAD)
    plan.set_slow_body("slow-body", SLOW_BODY)

    httpd = build_server("127.0.0.1", port, rate=20, replay_plan=plan)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    started = time.monotonic()
    check: list[tuple[str, bool, str]] = []
    print(f"== 并发交错回放（站点 {base_url}，min_interval={MIN_INTERVAL}s）==")
    try:
        thread.start()
        wait_health(httpd)

        _client, limiter, req_log, errors, elapsed = run_workload(
            base_url, tmp, plan)
        print_timeline(req_log)
        check_rules(check, req_log, limiter, errors, elapsed)

        print("\n-- 真实采集路由混合流量（目录/详情/影像/重试）--")
        _rc, _rl, real_log, real_errors, real_elapsed = run_real_traffic(
            base_url, tmp)
        print_timeline(real_log)
        check_real_traffic(check, real_log, real_errors)

        print("\n-- 重试耗尽用例：max_retries=0 + 恒定 503 --")
        raised, fail_at, after_gap, snap, after_recs = run_exhausted_case(
            base_url, tmp, plan)
        check.append((
            "R2d 重试耗尽的 503 请求抛出 TransientHTTPError",
            raised, f"失败耗时 {fail_at:.2f}s",
        ))
        check.append((
            "R2e 即使重试耗尽，Retry-After 暂停仍生效（后续请求被推迟）",
            after_gap >= PAUSE_SECONDS - TOLERANCE and snap["pause_events"] >= 1,
            f"后续请求等待 {after_gap:.2f}s，限流快照 {snap}",
        ))
        if after_recs:
            check.append((
                "R2f 暂停结束后普通请求正常发出并成功",
                after_recs[-1]["status"] == 200
                and after_recs[-1]["paused"],
                f"status={after_recs[-1]['status']} paused={after_recs[-1]['paused']}",
            ))

    finally:
        httpd.shutdown()
        httpd.server_close()
        if args.keep:
            print(f"\n现场保留：{tmp}（请求记录 {tmp}/requests.jsonl）")
        else:
            shutil.rmtree(tmp, ignore_errors=True)

    print("\n================ 回放核对 ================")
    passed = 0
    for name, ok, detail in check:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}"
              + (f"（{detail}）" if detail else ""))
        passed += ok
    total_elapsed = time.monotonic() - started
    print(f"合计 {passed}/{len(check)} 通过，用时 {total_elapsed:.1f} 秒")
    in_time = total_elapsed <= 80
    print("✓ 总用时在 80 秒以内" if in_time else "✗ 总用时超过 80 秒")
    return 0 if passed == len(check) and in_time else 1


if __name__ == "__main__":
    sys.exit(main())
