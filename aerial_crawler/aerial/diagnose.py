"""续采诊断：分段请求诊断记录（只追加）+ 只读诊断报告。

记录侧（采集进程持有写锁时写入）：
- ``manifest/diagnostics.jsonl`` 与 events.jsonl 同口径：每行一次 fsync 的
  JSON，采集期间可随时逐行读，绝不会读到半截行（坏行由读取侧跳过）。
- 每次逻辑请求（catalog / detail / probe / chunk）写一条 ``start`` 与一条
  ``done``；进程被 kill -9 时“有 start 无 done”的请求在报告里一律标
  “未知”，并用记录在案的区段路径 stat 旁证实际落盘情况。
- 复用的区段不产生 HTTP 请求，只写一条 ``done(result=reused)``。

查询侧（值班员运行，只读、不取写锁）：
- 只读取 manifest/current.json、events.jsonl、diagnostics.jsonl，
  以及记录中登记过的区段/影像路径（仅 ``stat``，绝不 open 影像，
  也不 rglob 扫描 images 目录、不读 logs/run.log）；
- 采集进程持有 flock 写锁时查询照常工作。
"""
from __future__ import annotations

import errno
import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# 请求种类
KIND_CATALOG = "catalog"
KIND_DETAIL = "detail"
KIND_PROBE = "probe"
KIND_CHUNK = "chunk"
KIND_IMAGE = "image"          # 整卷影像复用（无 HTTP 请求）

# 逻辑请求结果（done 记录）
RESULT_SERVED = "served"      # 实际发起 HTTP 并成功，区段已落盘
RESULT_REUSED = "reused"      # 未发请求，复用本地已持久化区段
RESULT_FAILED = "failed"      # 逻辑请求失败（重试耗尽 / 校验异常等）

# 报告侧派生状态
STATE_UNKNOWN = "unknown"     # 有 start 无 done：未获确证

# 落盘核对结论
DISK_CHUNK_PRESENT = "chunk_present"          # 区段文件存在且长度相符
DISK_CHUNK_MISSING = "chunk_missing"          # 区段文件不存在
DISK_ASSEMBLED = "assembled_committed"        # 区段已装配提交，目录被清理
DISK_REUSED_ASSEMBLED = "reused_assembled"    # 复用登记且已装配提交
DISK_NO_EVIDENCE = "no_evidence"              # 未知请求且无任何落盘证据
DISK_UNEXPECTED = "unexpected_present"        # 失败请求却留下了有效区段

# 运行状态
RUN_RUNNING = "running"
RUN_FINISHED = "finished"
RUN_CRASHED = "crashed"

# 版本归属
PUBLISH_CURRENT = "current"            # 该版本就是当前条目
PUBLISH_SUPERSEDED = "superseded"      # 曾发布，已被更新版本替换
PUBLISH_UNPUBLISHED = "unpublished"    # 从未进入当前清单

_FIRST_PUBLISH_EVENTS = ("download_succeeded", "replaced")


# ---------------------------------------------------------------------- #
# 只追加诊断存储
# ---------------------------------------------------------------------- #
class DiagnosticsStore:
    """diagnostics.jsonl 的只追加写入器（线程安全，每行 fsync）。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()

    def append(self, record: dict[str, Any]) -> None:
        record = dict(record)
        record.setdefault("ts_real", round(_now_mono(), 3))
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    def read_records(self) -> list[dict]:
        """只读回放；尾部半行 / 坏行直接跳过（原子写口径下正常不会出现）。"""
        if not self.path.exists():
            return []
        records: list[dict] = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return records

    def next_run_seq(self, mode: str) -> int:
        """历次运行序号取最大值 +1；新工作目录从 1 开始。"""
        seqs = [r.get("run_seq", 0) for r in self.read_records()
                if r.get("type") == "run"]
        return max(seqs, default=0) + 1


def _now_mono() -> float:
    import time
    return time.monotonic()


# ---------------------------------------------------------------------- #
# 请求追踪器：由采集端各线程调用
# ---------------------------------------------------------------------- #
class RequestTracker:
    """把逻辑请求的 start/done 与 HTTP 尝试过程写入 DiagnosticsStore。

    req_id 由 begin() 生成、显式传给 HTTP 客户端，尝试过程按 req_id 归集，
    避免多线程 / 退避睡眠期间线程本地变量串台。
    """

    def __init__(self, store: DiagnosticsStore, run_seq: int = 0) -> None:
        self.store = store
        self.run_seq = run_seq
        self._counter_lock = threading.Lock()
        self._counter = 0
        self._attempts_lock = threading.Lock()
        self._attempts: dict[str, list[dict]] = {}
        self._rounds: dict[str, int] = {}

    def _new_req_id(self) -> str:
        with self._counter_lock:
            self._counter += 1
            n = self._counter
        return f"r{self.run_seq}-{n:06d}"

    def begin(
        self,
        *,
        kind: str,
        target: str,
        round_no: int,
        volume_id: Optional[str] = None,
        version: Optional[int] = None,
        updated_at: Optional[int] = None,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        chunk_index: Optional[int] = None,
        chunk_path: Optional[str] = None,
        expected_len: Optional[int] = None,
        site_time: Optional[int] = None,
    ) -> str:
        req_id = self._new_req_id()
        with self._attempts_lock:
            self._attempts[req_id] = []
            self._rounds[req_id] = round_no
        self.store.append({
            "type": "request", "phase": "start",
            "req_id": req_id, "run_seq": self.run_seq, "round": round_no,
            "kind": kind, "target": target,
            "volume_id": volume_id, "version": version,
            "updated_at": updated_at,
            "range": [range_start, range_end] if range_start is not None else None,
            "chunk_index": chunk_index, "chunk_path": chunk_path,
            "expected_len": expected_len,
            "site_time": site_time,
        })
        return req_id

    def note_attempt(self, req_id: Optional[str], target: str, attempt: int,
                     status: Optional[int], error: Optional[str],
                     site_time: Optional[int]) -> None:
        """HTTP 客户端回调：记录单次底层尝试（含被重试的 503/500）。"""
        if req_id is None:
            return
        with self._attempts_lock:
            bucket = self._attempts.get(req_id)
            if bucket is None:
                return
            bucket.append({
                "attempt": attempt,
                "status": status,
                "error": error,
                "site_time": site_time,
            })

    def end(self, req_id: str, result: str, *,
            site_time: Optional[int] = None,
            volume_id: Optional[str] = None,
            version: Optional[int] = None,
            updated_at: Optional[int] = None,
            error: Optional[str] = None,
            extra: Optional[dict] = None) -> None:
        with self._attempts_lock:
            attempts = self._attempts.pop(req_id, [])
            round_no = self._rounds.pop(req_id, None)
        record = {
            "type": "request", "phase": "done",
            "req_id": req_id, "run_seq": self.run_seq,
            "round": round_no,
            "result": result, "attempts": attempts,
            "site_time": site_time,
            "volume_id": volume_id, "version": version,
            "updated_at": updated_at, "error": error,
        }
        if extra:
            record.update(extra)
        self.store.append(record)

    def record_reused(self, **fields: Any) -> None:
        """本地复用：没有 HTTP 在途窗口，直接落一条 done 记录。"""
        req_id = self._new_req_id()
        self.store.append({
            "type": "request", "phase": "done",
            "req_id": req_id, "run_seq": self.run_seq,
            "result": RESULT_REUSED, "attempts": [],
            "round": fields.pop("round_no", 0),
            "site_time": fields.pop("site_time", None),
            **fields,
        })


# ---------------------------------------------------------------------- #
# 只读工具
# ---------------------------------------------------------------------- #
def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _pid_alive(pid: Optional[int]) -> Optional[bool]:
    if not pid:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return None
    return True


def _stat_len(path: Path) -> Optional[int]:
    """只取元数据，绝不打开文件内容。"""
    try:
        return path.stat().st_size
    except OSError:
        return None


# ---------------------------------------------------------------------- #
# 报告构建（纯只读）
# ---------------------------------------------------------------------- #
@dataclass
class _RequestRow:
    req_id: str
    run_seq: int
    round_no: Optional[int]
    kind: str
    target: str
    volume_id: Optional[str]
    version: Optional[int]
    updated_at: Optional[int]
    range_start: Optional[int]
    range_end: Optional[int]
    chunk_index: Optional[int]
    chunk_path: Optional[str]
    expected_len: Optional[int]
    state: str
    attempts: list[dict] = field(default_factory=list)
    error: Optional[str] = None
    disk_state: Optional[str] = None
    disk_note: str = ""
    start_site_time: Optional[int] = None
    done_site_time: Optional[int] = None


def build_report(
    workdir: Path,
    *,
    volume_id: Optional[str] = None,
    version: Optional[int] = None,
    run_seq: Optional[int] = None,
) -> dict[str, Any]:
    """从工作目录的清单产物构建诊断报告（不写任何文件、不取锁）。"""
    manifest_dir = workdir / "manifest"
    state_dir = workdir / "state"
    diag_records = _read_jsonl(manifest_dir / "diagnostics.jsonl")
    events = _read_jsonl(manifest_dir / "events.jsonl")
    current = _read_json(manifest_dir / "current.json")
    current_broken = (manifest_dir / "current.json").exists() and current is None

    # ---- 运行清单与存活状态 ----
    runs_raw: dict[int, dict] = {}
    for r in diag_records:
        if r.get("type") != "run":
            continue
        seq = r.get("run_seq")
        entry = runs_raw.setdefault(seq, {
            "run_seq": seq, "mode": r.get("mode"), "pid": r.get("pid"),
            "started_real": r.get("ts_real"), "start_site_time": r.get("site_time"),
            "ended": False, "end_reason": None, "remaining_failed": None,
        })
    for r in diag_records:
        if r.get("type") == "run_end":
            entry = runs_raw.get(r.get("run_seq"))
            if entry is not None:
                entry["ended"] = True
                entry["end_reason"] = r.get("reason")
                entry["remaining_failed"] = r.get("remaining_failed")

    lock_pid: Optional[int] = None
    lock_path = state_dir / "lock"
    if lock_path.exists():
        raw = lock_path.read_text(encoding="utf-8").strip()
        if raw.isdigit():
            lock_pid = int(raw)

    runs: list[dict] = []
    for seq in sorted(runs_raw):
        entry = runs_raw[seq]
        pid = entry["pid"]
        alive = _pid_alive(pid)
        lock_held_by_this = lock_pid is not None and lock_pid == pid
        if entry["ended"]:
            status = RUN_FINISHED
        elif alive and lock_held_by_this:
            status = RUN_RUNNING
        elif alive:
            # 进程在但锁已不属于它（理论上不会发生，按运行中保守处理）
            status = RUN_RUNNING
        else:
            status = RUN_CRASHED
        entry["status"] = status
        entry["pid_alive"] = alive
        runs.append(entry)

    # ---- 合并 start/done 请求记录 ----
    starts: dict[str, dict] = {}
    dones: dict[str, dict] = {}
    catalog_rows_raw: list[dict] = []
    for r in diag_records:
        if r.get("type") != "request":
            continue
        rid = r.get("req_id")
        if r.get("phase") == "start":
            starts[rid] = r
        elif r.get("phase") == "done":
            dones[rid] = r

    def make_row(rid: str, start: Optional[dict], done: Optional[dict]) -> _RequestRow:
        src = done or start or {}
        rng = src.get("range") or (start or {}).get("range")
        return _RequestRow(
            req_id=rid,
            run_seq=src.get("run_seq") or (start or {}).get("run_seq"),
            round_no=src.get("round") if src.get("round") is not None
            else (start or {}).get("round"),
            kind=src.get("kind") or (start or {}).get("kind"),
            target=src.get("target") or (start or {}).get("target"),
            volume_id=src.get("volume_id") or (start or {}).get("volume_id"),
            version=src.get("version") if src.get("version") is not None
            else (start or {}).get("version"),
            updated_at=src.get("updated_at") or (start or {}).get("updated_at"),
            range_start=rng[0] if rng else None,
            range_end=rng[1] if rng else None,
            chunk_index=src.get("chunk_index") if src.get("chunk_index") is not None
            else (start or {}).get("chunk_index"),
            chunk_path=src.get("chunk_path") or (start or {}).get("chunk_path"),
            expected_len=src.get("expected_len") if src.get("expected_len") is not None
            else (start or {}).get("expected_len"),
            state=done.get("result") if done else STATE_UNKNOWN,
            attempts=(done or {}).get("attempts", []) or [],
            error=(done or {}).get("error"),
            start_site_time=(start or {}).get("site_time"),
            done_site_time=(done or {}).get("site_time"),
        )

    all_ids = set(starts) | set(dones)
    rows = [make_row(rid, starts.get(rid), dones.get(rid)) for rid in sorted(all_ids)]

    # ---- 首次发布轮次（事件流权威口径，原子替换后才写事件） ----
    first_publish: dict[tuple[str, int], dict] = {}
    last_commit_per_volume: dict[str, dict] = {}
    # 每个版本的全部复用记录（整卷复用 / 同址换版后旧版仍可回溯）
    reuse_history: dict[tuple[str, int], list[dict]] = {}
    for ev in events:
        vid = ev.get("volume_id")
        ver = ev.get("version")
        if vid is None or ver is None:
            continue
        key = (vid, ver)
        if ev.get("event") in _FIRST_PUBLISH_EVENTS and key not in first_publish:
            first_publish[key] = {
                "run_seq": ev.get("run_seq"),
                "round": ev.get("round"),
                "event": ev.get("event"),
                "site_time": ev.get("updated_at"),
            }
        if ev.get("event") in _FIRST_PUBLISH_EVENTS + ("reused",):
            last_commit_per_volume[vid] = ev
        if ev.get("event") == "reused":
            reuse_history.setdefault(key, []).append({
                "run_seq": ev.get("run_seq"),
                "round": ev.get("round"),
                "updated_at": ev.get("updated_at"),
            })

    # 已发布版本集合（含被替换的旧版），用于磁盘核对
    committed_versions: dict[tuple[str, int], dict] = {}
    for ev in events:
        if ev.get("event") in _FIRST_PUBLISH_EVENTS + ("reused",):
            committed_versions[(ev["volume_id"], ev["version"])] = ev

    # ---- 逐行磁盘核对（只 stat 记录在案的路径，不扫目录） ----
    def cross_check(row: _RequestRow) -> None:
        if row.kind != KIND_CHUNK:
            return
        cpath = Path(row.chunk_path) if row.chunk_path else None
        present = (
            cpath is not None
            and _stat_len(cpath) == row.expected_len
            and row.expected_len is not None
        )
        commit = committed_versions.get((row.volume_id, row.version)) if row.volume_id else None
        final_exists = False
        if commit and commit.get("image_path"):
            final_exists = _stat_len(Path(commit["image_path"])) is not None

        if row.state == RESULT_SERVED or row.state == RESULT_REUSED:
            if present:
                row.disk_state = DISK_CHUNK_PRESENT
                row.disk_note = f"区段存在（{row.expected_len}B）"
            elif commit and final_exists:
                if row.state == RESULT_REUSED:
                    row.disk_state = DISK_REUSED_ASSEMBLED
                    row.disk_note = "复用登记，区段已装配提交并清理，完整影像存在"
                else:
                    row.disk_state = DISK_ASSEMBLED
                    row.disk_note = "区段已装配提交并清理，完整影像存在"
            else:
                row.disk_state = DISK_CHUNK_MISSING
                row.disk_note = "登记的区段文件不存在且未见提交影像"
        elif row.state == RESULT_FAILED:
            if present:
                row.disk_state = DISK_UNEXPECTED
                row.disk_note = "请求登记失败，但同路径存在长度相符的区段（可能由后续重试补齐）"
            else:
                row.disk_state = DISK_CHUNK_MISSING
                row.disk_note = "未落盘（与失败登记一致）"
        else:  # unknown
            if present:
                row.disk_state = DISK_CHUNK_PRESENT
                row.disk_note = "请求未获确证，但区段文件实际存在且长度相符"
            else:
                row.disk_state = DISK_NO_EVIDENCE
                row.disk_note = "请求未获确证，且无区段落盘证据"

    for row in rows:
        cross_check(row)

    # ---- 按卷宗/版本聚合 ----
    vol_keys: dict[str, set[int]] = {}
    for row in rows:
        if row.volume_id:
            vol_keys.setdefault(row.volume_id, set()).add(row.version)
    for vid, entry in (current or {}).items():
        vol_keys.setdefault(vid, set()).add(entry.get("version"))
    for (vid, ver) in first_publish:
        vol_keys.setdefault(vid, set()).add(ver)
    for (vid, ver) in reuse_history:
        vol_keys.setdefault(vid, set()).add(ver)

    volumes_out: dict[str, dict] = {}
    for vid in sorted(vol_keys):
        cur_entry = (current or {}).get(vid)
        versions_out = []
        vers = sorted(v for v in vol_keys[vid] if v is not None)
        if not vers:
            vers = [None]  # 仅有残缺记录时
        for ver in vers:
            vrows = [r for r in rows if r.volume_id == vid
                     and (ver is None or r.version == ver)]
            targets = sorted({r.target for r in vrows if r.kind in
                              (KIND_CHUNK, KIND_PROBE, KIND_IMAGE) and r.target})
            publish = first_publish.get((vid, ver)) if ver is not None else None
            if cur_entry and ver is not None and cur_entry.get("version") == ver:
                publish_state = PUBLISH_CURRENT
            elif publish is not None:
                publish_state = PUBLISH_SUPERSEDED
            else:
                publish_state = PUBLISH_UNPUBLISHED
            versions_out.append({
                "version": ver,
                "updated_at": _first_value(vrows, "updated_at")
                or (cur_entry or {}).get("updated_at"),
                "image_target": targets[0] if targets else None,
                "first_publish": publish,
                "publish_state": publish_state,
                "reuse_history": reuse_history.get((vid, ver), []),
                "requests": [_row_to_dict(r) for r in vrows],
                "unknown_count": sum(1 for r in vrows if r.state == STATE_UNKNOWN),
            })
        volumes_out[vid] = {
            "current_entry": cur_entry,
            "image_exists": (_stat_len(Path(cur_entry["image_path"])) is not None
                             if cur_entry and cur_entry.get("image_path") else None),
            "versions": versions_out,
        }

    # ---- 发布一致性：事件流 vs current.json 实际状态 ----
    consistency: list[dict] = []
    if current_broken:
        consistency.append({"volume_id": None, "ok": False,
                            "detail": "current.json 无法解析（非原子写口径下不应出现）"})
    for vid in sorted(set(list((current or {}).keys())) | set(last_commit_per_volume)):
        cur_entry = (current or {}).get(vid)
        last_ev = last_commit_per_volume.get(vid)
        if cur_entry is None:
            consistency.append({"volume_id": vid, "ok": False,
                                "detail": "事件流存在提交记录，但 current.json 无该卷宗条目"})
            continue
        if last_ev is None:
            consistency.append({"volume_id": vid, "ok": False,
                                "detail": "current.json 有条目但事件流缺少提交事件"})
            continue
        problems = []
        if cur_entry.get("version") != last_ev.get("version"):
            problems.append(
                f"版本不一致 current=v{cur_entry.get('version')} "
                f"event=v{last_ev.get('version')}")
        if cur_entry.get("checksum") != last_ev.get("checksum"):
            problems.append("校验值不一致")
        img = cur_entry.get("image_path")
        if not img or _stat_len(Path(img)) is None:
            problems.append("当前影像路径不存在")
        consistency.append({
            "volume_id": vid,
            "ok": not problems,
            "detail": "事件提交与 current.json 一致" if not problems
            else "；".join(problems),
        })

    catalog_requests = [
        _row_to_dict(r) for r in rows if r.kind == KIND_CATALOG
    ]

    # ---- 过滤 ----
    if run_seq is not None:
        catalog_requests = [r for r in catalog_requests
                            if r["run_seq"] == run_seq]
        for vid, v in volumes_out.items():
            for ver in v["versions"]:
                ver["requests"] = [r for r in ver["requests"]
                                   if r["run_seq"] == run_seq]
        runs = [r for r in runs if r["run_seq"] == run_seq]
    if volume_id is not None:
        volumes_out = {vid: v for vid, v in volumes_out.items()
                       if vid == volume_id}
        consistency = [c for c in consistency
                       if c["volume_id"] == volume_id]
    if version is not None:
        for vid, v in volumes_out.items():
            v["versions"] = [x for x in v["versions"]
                             if x["version"] == version]

    crashed = [r for r in runs if r["status"] == RUN_CRASHED]
    inflight_unknown = sum(
        1 for r in rows if r.state == STATE_UNKNOWN
        and (run_seq is None or r.run_seq == run_seq)
        and (volume_id is None or r.volume_id == volume_id)
    )

    return {
        "workdir": str(workdir),
        "lock": {
            "path": str(lock_path),
            "held": lock_pid is not None and _pid_alive(lock_pid) is True,
            "pid": lock_pid,
            "pid_alive": _pid_alive(lock_pid),
        },
        "runs": runs,
        "crashed_run_seqs": [r["run_seq"] for r in crashed],
        "inflight_unknown": inflight_unknown,
        "catalog_requests": catalog_requests,
        "volumes": volumes_out,
        "consistency": consistency,
        "current_json_broken": current_broken,
        "filter": {"volume_id": volume_id, "version": version, "run_seq": run_seq},
    }


def _first_value(rows: list[_RequestRow], attr: str) -> Any:
    for r in rows:
        v = getattr(r, attr, None)
        if v is not None:
            return v
    return None


def _row_to_dict(r: _RequestRow) -> dict[str, Any]:
    return {
        "req_id": r.req_id, "run_seq": r.run_seq, "round": r.round_no,
        "kind": r.kind, "target": r.target,
        "volume_id": r.volume_id, "version": r.version,
        "updated_at": r.updated_at,
        "range": [r.range_start, r.range_end] if r.range_start is not None else None,
        "chunk_index": r.chunk_index, "chunk_path": r.chunk_path,
        "expected_len": r.expected_len,
        "state": r.state, "attempts": r.attempts, "error": r.error,
        "disk_state": r.disk_state, "disk_note": r.disk_note,
        "start_site_time": r.start_site_time, "done_site_time": r.done_site_time,
    }


# ---------------------------------------------------------------------- #
# 文本渲染
# ---------------------------------------------------------------------- #
_STATE_TEXT = {
    RESULT_SERVED: "已落盘",
    RESULT_REUSED: "复用",
    RESULT_FAILED: "失败",
    STATE_UNKNOWN: "未知",
}
_RUN_STATUS_TEXT = {
    RUN_RUNNING: "运行中（持有写锁）",
    RUN_FINISHED: "正常结束",
    RUN_CRASHED: "异常终止（疑似强杀/崩溃：无 run_end 且进程已退出）",
}
_PUBLISH_TEXT = {
    PUBLISH_CURRENT: "当前版本",
    PUBLISH_SUPERSEDED: "已被更新版本替换（旧版影像保留）",
    PUBLISH_UNPUBLISHED: "未发布（从未进入当前清单）",
}


def render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("续采诊断报告（只读查询，未获取写锁）")
    lines.append(f"工作目录: {report['workdir']}")
    lines.append("数据来源: manifest/current.json + events.jsonl + diagnostics.jsonl；"
                 "仅 stat 已登记路径，不读影像、不扫全量日志")
    lock = report["lock"]
    if lock["pid"] is None:
        lines.append("写锁: 无锁文件")
    elif lock["held"]:
        lines.append(f"写锁: 运行中（lock 指向 PID {lock['pid']}，进程存活，查询与采集并行）")
    else:
        lines.append(f"写锁: 空闲（lock 指向 PID {lock['pid']}，进程已退出）")

    if report["runs"]:
        lines.append("")
        lines.append("== 运行概览 ==")
        for r in report["runs"]:
            tail = ""
            if r["status"] == RUN_FINISHED and r["remaining_failed"] is not None:
                tail = f"，结束时待补齐 {r['remaining_failed']} 卷"
            lines.append(
                f"run {r['run_seq']}: 模式={r['mode']} PID={r['pid']} "
                f"状态={_RUN_STATUS_TEXT[r['status']]}{tail}"
            )
    else:
        lines.append("（暂无采集运行记录）")

    if report["catalog_requests"]:
        lines.append("")
        lines.append("== 运行级请求（目录页，无单卷归属） ==")
        lines.append("  运行 轮次 目标                          结果   尝试次数")
        for r in report["catalog_requests"]:
            lines.append(
                f"  r{r['run_seq']:<3} {_fmt_round(r['round'])} "
                f"{(r['target'] or '')[:30]:<30} "
                f"{_STATE_TEXT.get(r['state'], r['state']):<4} "
                f"{len(r['attempts'])}"
            )

    if not report["volumes"]:
        lines.append("")
        lines.append("（过滤条件下无卷宗记录）")
    for vid, v in report["volumes"].items():
        lines.append("")
        lines.append(f"== 卷宗 {vid} ==")
        cur = v.get("current_entry")
        if cur:
            exists = "存在" if v.get("image_exists") else "缺失"
            lines.append(
                f"当前条目: v{cur.get('version')} updated_at={cur.get('updated_at')} "
                f"checksum={str(cur.get('checksum'))[:12]}… "
                f"影像={cur.get('image_path')}（{exists}）"
            )
        else:
            lines.append("当前条目: 无（未进入 current.json）")
        for ver in v["versions"]:
            lines.append(
                f"  -- 版本 v{ver['version']} | updated_at={ver['updated_at']}"
                + (f" | 影像地址 {ver['image_target']}" if ver.get("image_target") else "")
            )
            fp = ver.get("first_publish")
            if fp:
                lines.append(
                    f"     首次进入当前清单: run {fp['run_seq']} 轮次 {_fmt_round(fp['round'])}"
                    f"（事件 {fp['event']}）"
                )
            else:
                lines.append("     首次进入当前清单: 无（该版本从未发布）")
            lines.append(f"     归属状态: {_PUBLISH_TEXT[ver['publish_state']]}")
            history = ver.get("reuse_history") or []
            if history:
                desc = "、".join(
                    f"run {h['run_seq']} {_fmt_round(h['round'])}" for h in history
                )
                lines.append(f"     复用记录（原归属与首次发布轮次保持可查）: {desc}")
            reqs = ver["requests"]
            chunk_reqs = [r for r in reqs if r["kind"] == KIND_CHUNK]
            other_reqs = [r for r in reqs if r["kind"] != KIND_CHUNK]
            if chunk_reqs:
                lines.append("     分段请求（每次 Range 请求一行）:")
                lines.append("       运行 轮次 区段   Range               结果   尝试 落盘核对")
                for r in chunk_reqs:
                    rng = (f"{r['range'][0]}-{r['range'][1]}"
                           if r.get("range") else "-")
                    lines.append(
                        f"       r{r['run_seq']:<3} {_fmt_round(r['round']):<4} "
                        f"#{_fmt_idx(r['chunk_index']):<4} {rng:<19} "
                        f"{_STATE_TEXT.get(r['state'], r['state']):<4} "
                        f"{len(r['attempts']):<4} {r.get('disk_note') or '-'}"
                    )
                    if r["state"] == STATE_UNKNOWN:
                        lines.append(
                            "              └ 未获确证（有请求开始记录、无完成记录）"
                        )
                    if r.get("error"):
                        lines.append(f"              └ 错误: {r['error']}")
            if other_reqs:
                lines.append("     其他请求（详情 / 探测 / 整卷复用）:")
                for r in other_reqs:
                    lines.append(
                        f"       r{r['run_seq']:<3} {_fmt_round(r['round']):<4} "
                        f"{r['kind']:<7} {(r['target'] or '')[:24]:<24} "
                        f"{_STATE_TEXT.get(r['state'], r['state']):<4} "
                        f"尝试 {len(r['attempts'])}"
                        + (f"  {r['disk_note']}" if r.get("disk_note") else "")
                    )
            if not reqs:
                lines.append("     （该版本无诊断请求记录）")

    lines.append("")
    lines.append("== 落盘与发布一致性核对 ==")
    for c in report["consistency"]:
        target = c["volume_id"] or "全局"
        mark = "✓" if c["ok"] else "✗"
        lines.append(f"  [{mark}] {target}: {c['detail']}")
    if report["inflight_unknown"]:
        lines.append(f"  提示: {report['inflight_unknown']} 条在途请求未获确证，已标“未知”"
                     "（磁盘旁证见各行落盘核对）")
    if not report["consistency"]:
        lines.append("  （无提交记录可核对）")
    return "\n".join(lines)


def _fmt_round(round_no: Optional[int]) -> str:
    return "主扫描" if round_no == 0 else ("补登轮" + str(round_no) if round_no else "-")


def _fmt_idx(idx: Optional[int]) -> str:
    return str(idx) if idx is not None else "-"


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
