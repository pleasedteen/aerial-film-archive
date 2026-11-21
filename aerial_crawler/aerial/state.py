"""工作目录持久化状态。

工作目录布局：
    workdir/
      state/state.json        扫描水位（模式、上次扫描站点时间、已知卷宗）
      state/failed.json       待补齐卷宗（下载/详情临时失败，含已用尝试次数）
      state/lock              单实例文件锁
      manifest/events.jsonl   追加式事件清单（采集期间也可安全读取）
      manifest/current.json   每卷当前条目（原子替换；始终指向校验通过的完整影像）
      images/<vid>/v<n>_<checksum>.img       校验通过的完整影像（不可变）
      images/<vid>/chunks/...                中断前已持久化的有效区段（续采用）
      logs/run.log

强制 kill 后状态文件本身用“临时文件 + os.replace”原子写入，绝不会出现半截 JSON。
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


@dataclass
class ScanState:
    mode: str = "none"                  # full / incremental
    # 上一次“扫描启动那一刻”的站点时间（增量水位）。
    # 不用扫描结束时刻：扫描期间生效的更新（updated_at <= 启动时刻）
    # 即使本次漏采，下次增量仍会被复查到，不会被水位永久跳过。
    last_scan_site_time: Optional[int] = None
    # 历次扫描见过的所有卷宗号
    known_volumes: set[str] = field(default_factory=set)

    @classmethod
    def load(cls, path: Path) -> "ScanState":
        if not path.exists():
            return cls()
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            mode=raw.get("mode", "none"),
            last_scan_site_time=raw.get("last_scan_site_time"),
            known_volumes=set(raw.get("known_volumes", [])),
        )

    def save(self, path: Path) -> None:
        payload = {
            "mode": self.mode,
            "last_scan_site_time": self.last_scan_site_time,
            "known_volumes": sorted(self.known_volumes),
        }
        atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


@dataclass
class FailedItem:
    """一个暂时失败、后续需要补齐的卷宗。"""
    volume_id: str
    href: Optional[str]                 # 详情地址；目录里见过但详情 404 时有值
    updated_at: Optional[int]
    attempts: int = 0

    def to_dict(self) -> dict:
        return {
            "volume_id": self.volume_id,
            "href": self.href,
            "updated_at": self.updated_at,
            "attempts": self.attempts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FailedItem":
        return cls(
            volume_id=d["volume_id"],
            href=d.get("href"),
            updated_at=d.get("updated_at"),
            attempts=int(d.get("attempts", 0)),
        )


class FailedQueue:
    """待补齐卷宗的持久化队列，按卷宗号去重（保留最大尝试次数）。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._items: dict[str, FailedItem] = {}
        if path.exists():
            for d in json.loads(path.read_text(encoding="utf-8")):
                item = FailedItem.from_dict(d)
                self._items[item.volume_id] = item

    def upsert(self, item: FailedItem) -> None:
        old = self._items.get(item.volume_id)
        if old and item.attempts < old.attempts:
            item.attempts = old.attempts
        self._items[item.volume_id] = item
        self._persist()

    def remove(self, volume_id: str) -> None:
        if self._items.pop(volume_id, None) is not None:
            self._persist()

    def all(self) -> list[FailedItem]:
        return list(self._items.values())

    def __len__(self) -> int:
        return len(self._items)

    def _persist(self) -> None:
        data = [item.to_dict() for item in self._items.values()]
        atomic_write_text(self.path, json.dumps(data, ensure_ascii=False, indent=2))


class WorkDir:
    """封装工作目录初始化、单实例锁与各文件路径。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.state_dir = root / "state"
        self.manifest_dir = root / "manifest"
        self.images_dir = root / "images"
        self.log_dir = root / "logs"
        for d in (self.state_dir, self.manifest_dir, self.images_dir, self.log_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "state.json"
        self.failed_file = self.state_dir / "failed.json"
        self.events_file = self.manifest_dir / "events.jsonl"
        self.current_file = self.manifest_dir / "current.json"
        self.diagnostics_file = self.manifest_dir / "diagnostics.jsonl"
        self.lock_file = self.state_dir / "lock"
        self._lock_fh = None

    def acquire_lock(self) -> None:
        fh = open(self.lock_file, "w")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            raise RuntimeError(f"工作目录 {self.root} 已被另一个采集进程占用")
        fh.write(str(os.getpid()))
        fh.flush()
        self._lock_fh = fh

    def release_lock(self) -> None:
        if self._lock_fh is not None:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            self._lock_fh.close()
            self._lock_fh = None
