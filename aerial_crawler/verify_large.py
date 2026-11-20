#!/usr/bin/env python3
"""大目录增量验证：200 页历史、3 处变化、增量代价受控。

覆盖三类要求：
  1. 有上限的翻页 —— 200 页历史目录里只有 3 处变化时，增量最多请求 4 个
     目录页（含补齐轮次），未变卷宗不拉详情；
  2. 停采后补课 —— 上次扫描水位 W=600、现在站点时间 3600（停采 50 分钟），
     仍要找回 W-120 更新的漏登卷宗、以及 updated_at 恰好等于 W 的换版；
  3. 强杀续采 —— 超大目录增量翻页途中 SIGKILL，同工作目录续采只从“已确认
     的下一页”继续，不重读已确认的目录页；翻页期间插入的条目最迟下一轮发现。

用法：
  python3 verify_large.py                 # 自动找空闲端口
  python3 verify_large.py --keep          # 保留临时工作目录
"""
from __future__ import annotations

import argparse
import json
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

VIRTUAL_EPOCH = 1_700_000_000
WATERMARK = VIRTUAL_EPOCH + 600          # 上一次扫描水位
PAGE_SIZE = 5

HISTORY_PAGES = 200


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_health(base_url: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base_url + "/healthz", timeout=1) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("模拟站未能在超时内就绪")


def _fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))


def stats(base_url: str) -> dict:
    return _fetch_json(base_url + "/stats")


def reset_stats(base_url: str) -> None:
    _fetch_json(base_url + "/stats/reset")


class Check:
    def __init__(self) -> None:
        self.items: list[tuple[str, bool, str]] = []

    def expect(self, name: str, ok: bool, detail: str = "") -> None:
        self.items.append((name, bool(ok), detail))
        print(f"  [{'✓' if ok else '✗'}] {name}" + (f" —— {detail}" if detail else ""))

    @property
    def all_ok(self) -> bool:
        return all(ok for _, ok, _ in self.items)


def start_server(port: int, log_path: Path, *,
                 history_pages: int, history_base: int, history_spacing: int,
                 last_scan_offset: int, missed_offset: int,
                 missed_seen_offset: int, start_site_time: int,
                 catalog_delay: float = 0.0) -> subprocess.Popen:
    cmd = [
        PYTHON, "-m", "mocksite", "--port", str(port), "--rate", "20",
        "--history-pages", str(history_pages),
        "--history-base", str(history_base),
        "--history-spacing", str(history_spacing),
        "--last-scan-offset", str(last_scan_offset),
        "--missed-offset", str(missed_offset),
        "--missed-seen-offset", str(missed_seen_offset),
        "--start-site-time", str(start_site_time),
        "--catalog-delay", str(catalog_delay),
    ]
    return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL,
                            stderr=open(log_path, "w"))


def crawl(base_url: str, workdir: Path, mode: str, log_path: Path,
          *, min_interval: float = 0.0, retry_rounds: int = 8,
          retry_round_interval: float = 2.0) -> subprocess.Popen:
    cmd = [
        PYTHON, "-m", "aerial",
        "--base-url", base_url,
        "--workdir", str(workdir),
        "--workers", "3",
        "--min-interval", str(min_interval),
        "--chunk-size", "4096",
        "--retry-rounds", str(retry_rounds),
        "--retry-round-interval", str(retry_round_interval),
        mode,
    ]
    return subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL,
                            stderr=open(log_path, "a"))


def seed_previous_scan(workdir: Path, *, history_base: int, history_spacing: int,
                       missed_offset: int, last_scan_offset: int,
                       missed_seen_offset: int,
                       watermark: int = WATERMARK,
                       forget: set[str] | None = None) -> None:
    """写入“上一次扫描已完成”的状态（等同一次全量扫描的产物）。

    基线取各卷在水平线那一秒对外可见的版本；目录尚未出现的卷宗（漏登）
    不在已知集合里。这样一个状态无法用一次真实全量扫描构造出来
    （同秒换版要求扫描在换版生效前读到旧版），因此这里直接落盘。
    """
    from mocksite.scenario import build_large_catalog
    volumes = build_large_catalog(
        HISTORY_PAGES,
        history_base=history_base,
        history_spacing=history_spacing,
        last_scan_offset=last_scan_offset,
        missed_offset=missed_offset,
        missed_seen_offset=missed_seen_offset,
    )
    # 上次扫描读到的是“水位前一秒”的目录快照：与水位同秒生效的换版
    # 当时还没被读到，必须留给本次增量找回
    observed_at = watermark - 1
    known: dict[str, int] = {}
    for vid, vol in volumes.items():
        if not vol.is_visible(observed_at):
            continue  # 漏登：上次扫描时目录里还没有它
        known[vid] = vol.version_at(observed_at).updated_at
    for vid in forget or set():
        known.pop(vid, None)
    state_dir = workdir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": "full",
        "last_scan_site_time": watermark,
        "known_volumes": sorted(known),
        "known_updated_at": dict(sorted(known.items())),
    }
    (state_dir / "state.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_current(workdir: Path) -> dict:
    path = workdir / "manifest" / "current.json"
    return json.loads(path.read_text("utf-8")) if path.exists() else {}


def load_scan_progress(workdir: Path) -> dict | None:
    path = workdir / "state" / "scan.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text("utf-8"))
    except ValueError:
        return None


def wait_exit(p: subprocess.Popen, timeout: float) -> int:
    try:
        return p.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        raise


def page_start_after(confirmed: int, pages: list[int]) -> bool:
    """续采主扫描片段是否从 confirmed+1 页开始且逐页递增。"""
    return bool(pages) and pages[0] == confirmed + 1


# ---------------------------------------------------------------------- #
def test_bounded_incremental(check: Check, tmp_root: Path) -> None:
    """200 页历史 + 3 处变化：增量 ≤4 个目录页、未变卷不查详情。"""
    print("场景 A：200 页历史目录，停采 50 分钟后做增量")
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    server = start_server(
        port, tmp_root / "serverA.log",
        history_pages=HISTORY_PAGES,
        history_base=0, history_spacing=60,
        last_scan_offset=600,        # 水位 = 600
        missed_offset=480,           # 漏登条目更新于 480（早于水位 120 秒）
        missed_seen_offset=620,      # 它 620 才上目录（仍在 10 分钟窗口内）
        start_site_time=VIRTUAL_EPOCH + 3600,  # 现在 = 3600，停采 50 分钟
    )
    wd = tmp_root / "workA"
    try:
        _wait_health(base_url)
        seed_previous_scan(wd, history_base=0, history_spacing=60,
                           missed_offset=480, last_scan_offset=600,
                           missed_seen_offset=620)
        reset_stats(base_url)
        p = crawl(base_url, wd, "incremental", tmp_root / "crawlA.log",
                  min_interval=0.0)
        rc = wait_exit(p, timeout=60)
        st = stats(base_url)
        cur = load_current(wd)
        log_text = (tmp_root / "crawlA.log").read_text("utf-8")

        check.expect("增量进程正常退出（exit=0）", rc == 0, f"returncode={rc}")
        check.expect(
            "增量最多请求 4 个目录页（含补齐轮次）",
            st["catalog"] <= 4,
            f"实际目录页请求 {st['catalog']} 次，页号 {st['catalog_pages']}",
        )
        check.expect(
            "未变卷宗不查详情（仅 3 处变化拉详情）",
            st["volume"] == 3,
            f"详情请求 {st['volume']} 次",
        )
        c1 = cur.get("C0001", {})
        c2 = cur.get("C0002", {})
        check.expect("换版 C0001 入库为 v2", c1.get("version") == 2,
                     f"version={c1.get('version')}")
        check.expect(
            "与扫描水位同秒的换版 C0002 也被找回",
            c2.get("version") == 2
            and c2.get("updated_at") == VIRTUAL_EPOCH + 600,
            f"version={c2.get('version')} updated_at={c2.get('updated_at')}",
        )
        check.expect(
            "停采 50 分钟后仍找回漏登卷宗 C0003",
            "C0003" in cur and cur["C0003"].get("updated_at") == VIRTUAL_EPOCH + 480,
            f"当前条目={'C0003' in cur} 更新于 480（水位前 120 秒）",
        )
        hist = [v for v in cur if v.startswith("H")]
        check.expect("历史卷宗未被重复拉取", not hist, f"历史条目 {len(hist)} 个")
        check.expect("日志记录了提前结束翻页", "增量提前结束翻页" in log_text)
    finally:
        server.send_signal(signal.SIGTERM)
        server.wait(timeout=5)


def test_crash_resume(check: Check, tmp_root: Path) -> None:
    """增量翻页途中 SIGKILL：续采不重读已确认的目录页。"""
    print("场景 B：增量翻页途中强杀，同工作目录续采")
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    # 历史卷宗更新时间全部落在补登地平线之上，增量必须翻完 40 页；
    # 每页 0.06 秒延迟，留出强杀的时间窗口。
    server = start_server(
        port, tmp_root / "serverB.log",
        history_pages=HISTORY_PAGES,
        history_base=550, history_spacing=1,
        last_scan_offset=600,
        missed_offset=480,
        missed_seen_offset=660,      # 翻页期间（站点 660）才上目录
        start_site_time=VIRTUAL_EPOCH + 600,
        catalog_delay=0.06,
    )
    wd = tmp_root / "workB"
    log_path = tmp_root / "crawlB.log"
    try:
        _wait_health(base_url)
        seed_previous_scan(wd, history_base=550, history_spacing=1,
                           missed_offset=480, last_scan_offset=600,
                           missed_seen_offset=660)
        reset_stats(base_url)
        p1 = crawl(base_url, wd, "incremental", log_path, min_interval=0.0)

        deadline = time.monotonic() + 30
        confirmed = 0
        while time.monotonic() < deadline:
            prog = load_scan_progress(wd)
            if prog:
                confirmed = int(prog.get("main_pages_done", 0))
            if confirmed >= 5:
                break
            if p1.poll() is not None:
                break
            time.sleep(0.02)
        check.expect("强杀前已确认至少 5 个目录页", confirmed >= 5,
                     f"已确认 {confirmed} 页")
        if p1.poll() is None:
            p1.send_signal(signal.SIGKILL)
        p1.wait(timeout=10)
        check.expect("采集进程被 SIGKILL 强杀", p1.returncode == -signal.SIGKILL,
                     f"returncode={p1.returncode}")

        pages_before = stats(base_url)["catalog_pages"]
        prog = load_scan_progress(wd) or {}
        next_url = prog.get("main_next_url") or ""
        confirmed_pages = int(prog.get("main_pages_done", 0))
        check.expect("断点记录了下一页地址", "page=" in next_url,
                     f"main_next_url={next_url}")

        p2 = crawl(base_url, wd, "incremental", log_path, min_interval=0.0)
        rc = wait_exit(p2, timeout=90)
        log_text = log_path.read_text("utf-8")
        all_pages = stats(base_url)["catalog_pages"]
        resumed_pages = all_pages[len(pages_before):]

        check.expect("续采进程正常退出（exit=0）", rc == 0, f"returncode={rc}")
        check.expect("续采沿用了目录断点", "沿用上次未完成的目录断点" in log_text)
        # 续采先接着翻主扫描（页号必须严格递增且从断点页开始），
        # 之后补齐轮次会从首页复查以捞取翻页期间插入 / 补登的条目。
        main_seg: list[int] = []
        for page in resumed_pages:
            if main_seg and page <= main_seg[-1]:
                break
            main_seg.append(page)
        expected = list(range(confirmed_pages + 1,
                              confirmed_pages + 1 + len(main_seg)))
        check.expect(
            "续采不重读已确认的目录页",
            main_seg == expected and page_start_after(confirmed_pages, main_seg),
            f"强杀前已确认 {confirmed_pages} 页，续采主扫描页号 {main_seg[:6]}"
            + (" ..." if len(main_seg) > 6 else ""),
        )
        check.expect(
            "续采从已确认页之后继续",
            bool(main_seg) and main_seg[0] == confirmed_pages + 1,
            f"续采首页 {main_seg[0] if main_seg else None}",
        )
        check.expect(
            "翻页期间插入的条目最迟下一轮被发现",
            "C0003" in load_current(wd) and "补登复查" in log_text,
            "漏登条目在站点 660 才上目录",
        )
        check.expect("续采最终无待补齐卷宗", "待补齐 0 卷" in log_text)
    finally:
        server.send_signal(signal.SIGTERM)
        server.wait(timeout=5)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="保留临时工作目录")
    args = ap.parse_args()

    tmp_root = Path(tempfile.mkdtemp(prefix="aerial_verify_large_"))
    check = Check()
    started = time.monotonic()
    print(f"== 大目录增量验证（工作目录 {tmp_root}）==")
    try:
        test_bounded_incremental(check, tmp_root)
        test_crash_resume(check, tmp_root)
    finally:
        if not args.keep:
            shutil.rmtree(tmp_root, ignore_errors=True)
        else:
            print(f"现场保留在 {tmp_root}")

    elapsed = time.monotonic() - started
    print("\n================ 验证报告 ================")
    passed = sum(1 for _, ok, _ in check.items if ok)
    for name, ok, detail in check.items:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"（{detail}）" if detail else ""))
    print(f"合计 {passed}/{len(check.items)} 通过，用时 {elapsed:.1f} 秒")
    return 0 if check.all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
