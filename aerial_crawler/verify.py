#!/usr/bin/env python3
"""一条命令完成的端到端验证（80 秒内输出可读结果）。

覆盖四类必测场景：
  1. 补登    —— V0007 在目录中迟到（更新时间后 120 站点秒才出现，10 分钟窗口内）；
  2. 限流    —— 模拟站滚动窗口 503 + Retry-After，采集端统一暂停、退避重试；
  3. 换版    —— V0006 换版后更新时间严格递增、下载地址沿用、清单原子替换不回退；
  4. 中断恢复 —— 全量下载中途 kill -9，同工作目录续采，复用已持久化的有效区段。

设计：模拟站虚拟时钟 20 倍速，站点口径 10 分钟 ≈ 真实 30 秒。
用法：
  python3 verify.py                 # 自动找空闲端口、建临时目录
  python3 verify.py --port 18099 --workdir /tmp/verify_data --keep
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
# 站点偏移（虚拟秒）阈值
V6_V2_AT = 1_700_000_000 + 700
V7_FIRST_SEEN = 1_700_000_000 + 180


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_health(base_url: str, timeout: float = 10.0) -> None:
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base_url + "/healthz", timeout=1) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError("模拟站未能在超时内就绪")


def _site_time(base_url: str) -> int:
    import urllib.request
    with urllib.request.urlopen(base_url + "/healthz", timeout=2) as r:
        return int(r.headers["X-Site-Time"])


def _wait_site_time(base_url: str, target: int, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _site_time(base_url) >= target:
            return
        time.sleep(0.5)
    raise RuntimeError(f"站点时间未达到 {target}")


class Check:
    """收集断言结果，最后统一输出可读报告。"""

    def __init__(self) -> None:
        self.items: list[tuple[str, bool, str]] = []

    def expect(self, name: str, ok: bool, detail: str = "") -> None:
        self.items.append((name, bool(ok), detail))
        mark = "✓" if ok else "✗"
        print(f"  [{mark}] {name}" + (f" —— {detail}" if detail else ""))

    @property
    def all_ok(self) -> bool:
        return all(ok for _, ok, _ in self.items)


def _catalog_nonempty(base_url: str, timeout: float = 15.0) -> bool:
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base_url + "/catalog", timeout=2) as r:
                if b"entry-link" in r.read():
                    return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def crawl(base_url: str, workdir: Path, mode: str, log_path: Path,
          chunk_size: int = 4096) -> subprocess.Popen:
    cmd = [
        PYTHON, "-m", "aerial",
        "--base-url", base_url,
        "--workdir", str(workdir),
        "--workers", "3",
        "--min-interval", "0.03",
        "--chunk-size", str(chunk_size),
        "--retry-rounds", "8",
        "--retry-round-interval", "2",
        mode,
    ]
    return subprocess.Popen(
        cmd, cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=open(log_path, "a"),
    )


def load_current(workdir: Path) -> dict:
    return json.loads((workdir / "manifest" / "current.json").read_text("utf-8"))


def diagnose(workdir: Path, *extra: str) -> dict:
    """以只读子进程运行诊断，返回 JSON 报告（不获取写锁）。"""
    cmd = [PYTHON, "-m", "aerial", "--workdir", str(workdir),
           "diagnose", "--json", *extra]
    out = subprocess.run(cmd, cwd=ROOT, check=True,
                         stdout=subprocess.PIPE, text=True)
    return json.loads(out.stdout)


def _all_requests(report: dict) -> list[dict]:
    out = []
    for v in report["volumes"].values():
        for ver in v["versions"]:
            out.extend(ver["requests"])
    return out


def _version_view(report: dict, vid: str, version: int) -> dict:
    for ver in report["volumes"][vid]["versions"]:
        if ver["version"] == version:
            return ver
    raise AssertionError(f"报告缺少 {vid} v{version}")


def verify_all_images(workdir: Path, check: Check, expect_count: int) -> None:
    cur = load_current(workdir)
    check.expect(f"当前条目数为 {expect_count}", len(cur) == expect_count,
                 f"实际 {len(cur)}")
    bad = []
    for vid, e in cur.items():
        p = Path(e["image_path"])
        if not p.exists():
            bad.append(f"{vid} 文件缺失")
            continue
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        if digest != e["checksum"] or p.stat().st_size != e["size"]:
            bad.append(f"{vid} 校验不符")
    check.expect("每卷当前影像 sha256/大小均校验通过", not bad,
                 "; ".join(bad) or f"{len(cur)} 卷全部通过")
    incomplete = [v for v, e in cur.items()
                  if not all(k in e for k in
                             ("version", "updated_at", "checksum", "image_path"))]
    check.expect("每卷恰好一个字段完整的当前条目", not incomplete)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--workdir", type=Path, default=None)
    ap.add_argument("--keep", action="store_true", help="结束后保留临时目录")
    args = ap.parse_args()

    port = args.port or _free_port()
    base_url = f"http://127.0.0.1:{port}"
    tmp_root = args.workdir or Path(tempfile.mkdtemp(prefix="aerial_verify_"))
    tmp_root.mkdir(parents=True, exist_ok=True)
    wd = tmp_root / "work"
    log_path = tmp_root / "crawl_stderr.log"
    check = Check()
    started = time.monotonic()

    print(f"== 航拍采集端到端验证（端口 {port}，工作目录 {tmp_root}）==")
    server = subprocess.Popen(
        [PYTHON, "-m", "mocksite", "--port", str(port), "--rate", "20"],
        cwd=ROOT, stdout=subprocess.DEVNULL,
        stderr=open(tmp_root / "server.log", "w"),
    )
    try:
        _wait_health(base_url)

        # ---- 阶段 1：全量采集中途 kill -9 ----
        print("阶段 1：全量采集中途 SIGKILL，随后同目录续采")
        assert _catalog_nonempty(base_url), "模拟站目录迟迟未出现条目"
        p = crawl(base_url, wd, "full", log_path)
        deadline = time.monotonic() + 15
        chunk_seen = False
        while time.monotonic() < deadline:
            images_root = wd / "images"
            chunks = list(images_root.rglob("chunk_*")) if images_root.exists() else []
            if chunks:
                chunk_seen = True
                break
            if p.poll() is not None:
                break
            time.sleep(0.1)
        chunks_before = (list((wd / "images").rglob("chunk_*"))
                         if (wd / "images").exists() else [])
        check.expect("中断前已有持久化区段落盘", chunk_seen,
                     f"{len(chunks_before)} 段")
        if p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait(timeout=10)
        check.expect("采集进程被 SIGKILL 强杀", p.returncode == -signal.SIGKILL,
                     f"returncode={p.returncode}")

        # ---- 阶段 1.5：强杀后立即只读诊断（不持有写锁） ----
        print("阶段 1.5：强杀后运行只读续采诊断")
        report_killed = diagnose(wd)
        run1 = next((r for r in report_killed["runs"] if r["run_seq"] == 1), None)
        check.expect("诊断把无 run_end 且进程已退出的运行标为异常终止",
                     run1 is not None and run1["status"] == "crashed",
                     f"runs={[(r['run_seq'], r['status']) for r in report_killed['runs']]}")
        check.expect("强杀在途的分段请求未获确证，一律标“未知”",
                     report_killed["inflight_unknown"] >= 1,
                     f"未知请求 {report_killed['inflight_unknown']} 条")
        killed_reqs = _all_requests(report_killed)
        unknowns = [q for q in killed_reqs if q["state"] == "unknown"]
        check.expect("未知请求只可能来自分段/详情/探测，且每条均有磁盘旁证结论",
                     all(q["kind"] in ("chunk", "detail", "probe")
                         and q["disk_state"] in
                         ("chunk_present", "no_evidence", None) for q in unknowns),
                     f"{len(unknowns)} 条未知")
        # 已 fsync 的区段：诊断只 stat 登记路径即可确认落盘（不打开影像）
        served_present = [q for q in killed_reqs
                          if q["state"] == "served"
                          and q["disk_state"] == "chunk_present"]
        check.expect("已确证请求的落盘区段可被 stat 旁证（不读影像、不扫全量日志）",
                     bool(served_present), f"{len(served_present)} 段在盘")
        check.expect("诊断报告不触碰锁文件之外的写路径（lock 仍指向被强杀 PID）",
                     report_killed["lock"]["held"] is False
                     and report_killed["lock"]["pid"] == p.pid)

        # ---- 阶段 2：等到补登条目出现后，用同一工作目录续采 ----
        # （区段复用立即发生；补登条目此时已在目录，主扫描即可发现，
        #  即使尚未出现，补齐轮次也会按地平线持续复查）
        _wait_site_time(base_url, V7_FIRST_SEEN + 2)
        p2 = crawl(base_url, wd, "full", log_path)
        try:
            p2.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p2.kill()
            raise
        check.expect("续采进程正常退出（exit=0）", p2.returncode == 0,
                     f"returncode={p2.returncode}")
        stderr_log = log_path.read_text("utf-8")
        check.expect("续采复用了中断前已持久化的有效区段",
                     "复用" in stderr_log, "日志含“复用 N 段”")
        check.expect("补登卷宗 V0007 经补齐轮次入库", "V0007" in load_current(wd))
        verify_all_images(wd, check, expect_count=8)

        # 续采后的诊断：崩溃/正常运行并存，复用行与首次发布轮次可查
        report_resume = diagnose(wd)
        statuses = {r["run_seq"]: r["status"] for r in report_resume["runs"]}
        check.expect("诊断区分强杀运行与续采运行（crashed + finished）",
                     statuses.get(1) == "crashed" and statuses.get(2) == "finished",
                     str(statuses))
        reused_rows = [q for q in _all_requests(report_resume)
                       if q["state"] == "reused"]
        check.expect("续采复用的区段在报告中单独标“复用”，与新拉请求区分",
                     bool(reused_rows), f"{len(reused_rows)} 条复用记录")
        v8 = _version_view(report_resume, "V0008", 1)
        check.expect("报告给出该版首次进入当前清单的运行与轮次",
                     v8["first_publish"] is not None
                     and v8["first_publish"]["run_seq"] == 2
                     and v8["first_publish"]["round"] == 0,
                     str(v8["first_publish"]))
        check.expect("强杀时的未知请求在续采后仍保留“未知”结论（不被重写）",
                     any(q["state"] == "unknown" for q in _all_requests(report_resume)))
        check.expect("落盘与发布一致性核对全部通过",
                     all(c["ok"] for c in report_resume["consistency"]),
                     "; ".join(c["detail"] for c in report_resume["consistency"]
                               if not c["ok"]))

        # ---- 阶段 3：等换版生效后跑增量 ----
        print("阶段 2：等待站点时钟越过换版时间点，执行增量采集")
        _wait_site_time(base_url, V6_V2_AT + 5)
        p3 = crawl(base_url, wd, "incremental", log_path)
        # 增量采集持有写锁期间并行查询：诊断不取锁，必须立刻成功
        locked_report = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and p3.poll() is None:
            try:
                locked_report = diagnose(wd, "--volume", "V0006")
                break
            except subprocess.CalledProcessError:
                time.sleep(0.2)
        check.expect("采集持有写锁时诊断查询照常返回（不抢锁、不读影像）",
                     locked_report is not None
                     and locked_report["lock"]["held"] is True)
        try:
            p3.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p3.kill()
            raise
        check.expect("增量采集正常退出（exit=0）", p3.returncode == 0,
                     f"returncode={p3.returncode}")

        stderr_log = log_path.read_text("utf-8")
        cur = load_current(wd)
        events = [json.loads(x) for x in
                  (wd / "manifest" / "events.jsonl").read_text().splitlines()]

        # 换版后的诊断：同址换版，旧版归属与首次发布轮次仍可查
        report_final = diagnose(wd)
        v6_views = {x["version"]: x
                    for x in report_final["volumes"]["V0006"]["versions"]}
        check.expect("换版后当前条目为 v2，旧版 v1 仍在报告中保留归属",
                     v6_views[2]["publish_state"] == "current"
                     and v6_views[1]["publish_state"] == "superseded")
        check.expect("旧版 v1 的首次发布轮次不被换版覆盖（仍指向 run 2 主扫描）",
                     v6_views[1]["first_publish"] is not None
                     and v6_views[1]["first_publish"]["run_seq"] == 2,
                     str(v6_views[1]["first_publish"]))
        check.expect("v2 首次进入当前清单记录为增量运行的发布事件",
                     v6_views[2]["first_publish"] is not None
                     and v6_views[2]["first_publish"]["run_seq"] == 3
                     and v6_views[2]["first_publish"]["event"] == "replaced",
                     str(v6_views[2]["first_publish"]))
        check.expect("新旧版本影像地址相同（同址换版），报告按版本分行",
                     len({x.get("image_target") for x in v6_views.values()
                          if x.get("image_target")}) == 1
                     and "/images/V0006" in (v6_views[2].get("image_target") or ""))
        check.expect("每次分段请求逐行可查，结果只含 已落盘/复用/失败/未知 四类",
                     {q["state"] for q in _all_requests(report_final)}
                     <= {"served", "reused", "failed", "unknown"})
        check.expect("最终一致性核对全部通过、current.json 结构完好",
                     all(c["ok"] for c in report_final["consistency"])
                     and not report_final["current_json_broken"])

        print("\n---- 场景断言 ----")
        print("[补登]")
        check.expect("V0007 迟到条目最终补齐且校验通过", "V0007" in cur,
                     "目录在更新时间后 120 站点秒才出现该条目")
        check.expect("JSONL 事件流可被编目员逐行读取",
                     all(isinstance(e, dict) and "event" in e for e in events),
                     f"{len(events)} 条事件")

        print("[限流与重试]")
        check.expect("503 Retry-After 触发全站统一暂停",
                     "站点要求暂停" in stderr_log)
        check.expect("500 临时故障经指数退避重试后恢复",
                     "临时故障（HTTP 500）" in stderr_log)
        check.expect("最终无待补齐卷宗", "待补齐 0 卷" in stderr_log)

        print("[换版]")
        v6 = cur.get("V0006", {})
        check.expect("V0006 当前为 v2 且更新时间严格递增",
                     v6.get("version") == 2 and v6.get("updated_at") == V6_V2_AT,
                     f"version={v6.get('version')} updated_at={v6.get('updated_at')}")
        v6_dir = wd / "images" / "V0006"
        finals = sorted(p.name for p in v6_dir.glob("*.img"))
        check.expect("新旧两版影像均保留（替换件通过前旧件可查）",
                     len(finals) == 2 and any(n.startswith("v1_") for n in finals)
                     and any(n.startswith("v2_") for n in finals),
                     ", ".join(finals))
        check.expect("下载地址沿用（路由按卷宗号，清单指向新 v2 文件）",
                     "v2_" in v6.get("image_path", ""))
        check.expect("清单未回退（当前条目始终指向 v2）",
                     v6.get("version") == 2)
        check.expect("事件流记录了 replaced 替换事件",
                     any(e.get("event") == "replaced"
                         and e.get("volume_id") == "V0006" for e in events))

        print("[中断恢复]")
        import re
        reuse = [int(m) for m in re.findall(r"复用 (\d+) 段", stderr_log)]
        check.expect("续采实际复用的已持久化区段数 ≥ 1",
                     bool(reuse) and max(reuse) >= 1,
                     f"各卷复用数 {reuse}")
        # 状态文件可被再次加载，证明原子写入没有产生半截 JSON
        json.loads((wd / "state" / "state.json").read_text("utf-8"))
        failed_path = wd / "state" / "failed.json"
        failed = json.loads(failed_path.read_text("utf-8")) if failed_path.exists() else []
        check.expect("状态文件结构完好、失败队列已清空", failed == [])

    finally:
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
        if not args.keep and args.workdir is None:
            shutil.rmtree(tmp_root, ignore_errors=True)

    elapsed = time.monotonic() - started
    print("\n================ 验证报告 ================")
    passed = sum(1 for _, ok, _ in check.items if ok)
    for name, ok, detail in check.items:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"（{detail}）" if detail else ""))
    print(f"合计 {passed}/{len(check.items)} 通过，用时 {elapsed:.1f} 秒")
    in_time = elapsed <= 80
    print("✓ 总用时在 80 秒以内" if in_time else "✗ 总用时超过 80 秒")
    return 0 if check.all_ok and in_time else 1


if __name__ == "__main__":
    sys.exit(main())
