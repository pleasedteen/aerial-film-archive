"""JSON Lines 清单与每卷“当前条目”原子视图。

两类产物：
1. events.jsonl —— 只追加的事件流（discovered / download_succeeded /
   download_failed / replaced / superseded / reused）。每行一次 fsync，
   采集期间编目员可以随时 tail / 读取，不会读到半行。
2. current.json —— {volume_id: 当前条目} 的原子替换视图。
   每个卷宗始终只有一个当前条目，且只在校验通过后才切换；
   “较晚完成的旧版下载”无法让清单回退（版本号/更新时间单调保护）。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional

from .parsing import VolumeDetail
from .state import atomic_write_text

# 事件类型
EV_DISCOVERED = "discovered"
EV_DOWNLOAD_SUCCEEDED = "download_succeeded"
EV_DOWNLOAD_FAILED = "download_failed"
EV_REPLACED = "replaced"          # 新版本校验通过，替换旧版当前条目
EV_SUPERSEDED = "superseded"      # 旧版下载完成但已被更新版本取代，不回退
EV_REUSED = "reused"              # 续采时复用已校验通过的完整影像


class Manifest:
    def __init__(self, events_file: Path, current_file: Path) -> None:
        self.events_file = events_file
        self.current_file = current_file
        self._lock = threading.Lock()
        self._current: dict[str, dict] = {}
        if current_file.exists():
            self._current = json.loads(current_file.read_text(encoding="utf-8"))
        if not events_file.exists():
            events_file.touch()

    # ------------------------------------------------------------------ #
    def _append_event(self, event: dict) -> None:
        line = json.dumps(event, ensure_ascii=False, sort_keys=True)
        with open(self.events_file, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def record_discovered(self, detail: VolumeDetail) -> None:
        with self._lock:
            self._append_event(
                {
                    "event": EV_DISCOVERED,
                    "volume_id": detail.volume_id,
                    "version": detail.version,
                    "updated_at": detail.updated_at,
                    "checksum": detail.checksum,
                    "size": detail.size,
                }
            )

    def record_failure(self, volume_id: str, detail: Optional[VolumeDetail], reason: str) -> None:
        with self._lock:
            self._append_event(
                {
                    "event": EV_DOWNLOAD_FAILED,
                    "volume_id": volume_id,
                    "version": detail.version if detail else None,
                    "updated_at": detail.updated_at if detail else None,
                    "reason": reason,
                }
            )

    # ------------------------------------------------------------------ #
    def current_entry(self, volume_id: str) -> Optional[dict]:
        with self._lock:
            return self._current.get(volume_id)

    def has_good_image(self, volume_id: str, checksum: str) -> Optional[str]:
        """若该 checksum 的完整影像已在当前条目中，返回其路径。"""
        with self._lock:
            entry = self._current.get(volume_id)
            if entry and entry.get("checksum") == checksum and os.path.exists(entry["image_path"]):
                return entry["image_path"]
        return None

    def commit_image(
        self,
        detail: VolumeDetail,
        image_path: Path,
        *,
        reused: bool = False,
    ) -> str:
        """校验通过后提交影像，返回结果类型。

        - replaced：该卷宗首次入库，或新版本（更新时间严格更大）替换旧版；
        - superseded：该版本已被更新版本取代（旧版晚完成），清单不回退。

        旧影像文件保留在磁盘上，替换完成前编目员始终能通过 current.json
        访问旧件；本方法只在新件校验通过后才切换指针。
        """
        new_entry = {
            "volume_id": detail.volume_id,
            "version": detail.version,
            "updated_at": detail.updated_at,
            "checksum": detail.checksum,
            "size": detail.size,
            "image_path": str(image_path),
        }
        with self._lock:
            old = self._current.get(detail.volume_id)
            if old is not None and old["updated_at"] > detail.updated_at:
                # 单调保护：较晚完成的旧版不能让清单回退
                self._append_event(
                    {
                        "event": EV_SUPERSEDED,
                        "volume_id": detail.volume_id,
                        "version": detail.version,
                        "updated_at": detail.updated_at,
                        "kept_version": old["version"],
                        "kept_updated_at": old["updated_at"],
                    }
                )
                return EV_SUPERSEDED

            event_type = EV_REUSED if reused else (
                EV_REPLACED if old is not None else EV_DOWNLOAD_SUCCEEDED
            )
            self._current[detail.volume_id] = new_entry
            self._persist_locked()
            self._append_event({
                "event": event_type,
                **new_entry,
                "previous_version": old["version"] if old else None,
            })
            return event_type

    def _persist_locked(self) -> None:
        atomic_write_text(
            self.current_file,
            json.dumps(self._current, ensure_ascii=False, indent=2, sort_keys=True),
        )

    # ------------------------------------------------------------------ #
    def volume_count(self) -> int:
        with self._lock:
            return len(self._current)

    def snapshot(self) -> dict[str, dict]:
        with self._lock:
            return json.loads(json.dumps(self._current))  # 深拷贝
