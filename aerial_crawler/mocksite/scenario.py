"""模拟站场景定义：卷宗、版本、确定性影像内容与故障注入。

时间口径全部为“虚拟站点秒”（从虚拟纪元 1_700_000_000 起算）。
速率默认 20 倍：真实 1 秒 = 站点 20 秒，于是站点口径的 10 分钟补登窗口
在真实环境里约 30 秒，验证脚本可在 80 秒内完整覆盖全部场景。
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field

VIRTUAL_EPOCH = 1_700_000_000
DEFAULT_RATE = 20
PAGE_SIZE = 5


@dataclass(frozen=True)
class ImageVersion:
    version: int
    updated_at: int          # 按秒的版本更新时间（虚拟站点秒）
    size: int
    seed: int
    checksum: str
    # 该版本影像首次可下载的虚拟时间（None 表示随卷宗立即可用）
    visible_at: int | None = None


@dataclass(frozen=True)
class Volume:
    volume_id: str
    versions: list[ImageVersion]
    # 目录条目（含详情页）首次出现的虚拟时间；用于模拟“最迟 10 分钟内补登”
    first_seen_at: int
    # 影像故障注入：按请求序号返回的一次性状态码（用完即失效）
    image_fail_sequence: tuple[int, ...] = field(default_factory=tuple)

    def version_at(self, site_time: int) -> ImageVersion:
        """站点时间 site_time 时对外可见的最新版本（更新时间已到才生效）。"""
        current = self.versions[0]
        for ver in self.versions:
            if ver.updated_at <= site_time:
                current = ver
        return current

    def is_visible(self, site_time: int) -> bool:
        return self.first_seen_at <= site_time


def _make_image(seed: int, size: int) -> bytes:
    rng = random.Random(seed)
    return bytes(rng.getrandbits(8) for _ in range(size))


def _ver(version: int, updated_at: int, size: int, seed: int,
         visible_at: int | None = None) -> ImageVersion:
    data = _make_image(seed, size)
    return ImageVersion(
        version=version,
        updated_at=updated_at,
        size=size,
        seed=seed,
        checksum=hashlib.sha256(data).hexdigest(),
        visible_at=visible_at,
    )


def build_volumes() -> dict[str, Volume]:
    """固定场景，顺序即目录排序（按最新更新时间倒序由 server 负责）。

    所有时间戳均为绝对虚拟站点秒（VIRTUAL_EPOCH + 偏移），
    与响应头 X-Site-Time 同口径，服务端重启后时钟依然连续可回放。
    """
    t = lambda offset: VIRTUAL_EPOCH + offset  # noqa: E731
    volumes = [
        # V0001-V0005：首批可见的普通卷宗
        Volume("V0001", [_ver(1, t(100), 100 * 1024, 1001)], first_seen_at=t(20)),
        # V0002：影像头两次请求临时 500，验证退避重试
        Volume("V0002", [_ver(1, t(110), 100 * 1024, 1002)],
               first_seen_at=t(20), image_fail_sequence=(500, 500)),
        Volume("V0003", [_ver(1, t(120), 100 * 1024, 1003)], first_seen_at=t(20)),
        Volume("V0004", [_ver(1, t(130), 100 * 1024, 1004)], first_seen_at=t(20)),
        # V0005：影像首次请求强制 503 + Retry-After，验证统一暂停
        Volume("V0005", [_ver(1, t(140), 100 * 1024, 1005)],
               first_seen_at=t(20), image_fail_sequence=(503,)),
        # V0006：换版——v2 更新时间严格递增，下载地址沿用 /images/V0006
        Volume(
            "V0006",
            [
                _ver(1, t(150), 100 * 1024, 1006),
                _ver(2, t(700), 120 * 1024, 2006),
            ],
            first_seen_at=t(20),
        ),
        # V0007：补登卷宗——更新时间偏移 60，但目录/详情在站点偏移 180 才出现
        # （180 - 60 = 120 秒 < 600 秒窗口）；影像再晚 60 站点秒开放
        Volume(
            "V0007",
            [_ver(1, t(60), 100 * 1024, 1007, visible_at=t(240))],
            first_seen_at=t(180),
        ),
        # V0008：用于中断恢复验证的大卷宗（区段多，下载中途被 kill）
        Volume("V0008", [_ver(1, t(90), 400 * 1024, 1008)], first_seen_at=t(20)),
    ]
    return {v.volume_id: v for v in volumes}


def build_large_catalog(history_pages: int,
                        history_base: int = 0,
                        history_spacing: int = 60,
                        last_scan_offset: int = 600,
                        missed_offset: int = 480,
                        missed_seen_offset: int = 620) -> dict[str, Volume]:
    """大目录场景：history_pages 页历史卷宗 + 3 处变化。

    用于验证增量不会翻完整个目录：
      - H####：历史卷宗，更新时间从 history_base 起每卷依次变旧
        （跨度 history_spacing 秒；为 0 时全部同一时刻，用于验证续采），
        占满 history_pages 页中的其余位置；
      - C0001：换版（v1 已在上次扫描时采到，v2 更新于 last_scan_offset+50）；
      - C0002：与上次扫描水位同秒的换版（v2 恰好更新于 last_scan_offset）；
      - C0003：漏登卷宗（更新时间 missed_offset，早于水位但目录迟到到
        missed_seen_offset 才出现，仍在补登窗口内）。
    历史卷宗数按 PAGE_SIZE 补足，保证总条目数正好 history_pages 页。
    """
    t = lambda offset: VIRTUAL_EPOCH + offset  # noqa: E731
    changes = [
        # 换版：v1 在上次扫描时已采，v2 是扫描之后的新版本
        Volume(
            "C0001",
            [_ver(1, t(100), 2048, 3001),
             _ver(2, t(last_scan_offset + 50), 2048, 4001)],
            first_seen_at=t(-100_000),
        ),
        # 与上次扫描水位同秒的换版：v1 更新于水位 -10，v2 恰好等于水位
        Volume(
            "C0002",
            [_ver(1, t(last_scan_offset - 10), 2048, 3002),
             _ver(2, t(last_scan_offset), 2048, 4002)],
            first_seen_at=t(-100_000),
        ),
        # 漏登：更新时间早于水位，但目录迟到才出现
        Volume(
            "C0003",
            [_ver(1, t(missed_offset), 2048, 3003)],
            first_seen_at=t(missed_seen_offset),
        ),
    ]
    history_count = max(0, history_pages * PAGE_SIZE - len(changes))
    history = [
        Volume(f"H{i:05d}",
               [_ver(1, t(history_base - history_spacing * i), 64, 9000 + i)],
               first_seen_at=t(-100_000))
        for i in range(1, history_count + 1)
    ]
    return {v.volume_id: v for v in changes + history}


class Scenario:
    def __init__(self, rate: int = DEFAULT_RATE, history_pages: int = 0,
                 start_site_time: int | None = None,
                 history_base: int = 0,
                 history_spacing: int = 60,
                 last_scan_offset: int = 600,
                 missed_offset: int = 480,
                 missed_seen_offset: int = 620) -> None:
        self.rate = rate
        self._start_site_time = start_site_time
        self.history_pages = history_pages
        if history_pages:
            self.volumes = build_large_catalog(
                history_pages,
                history_base=history_base,
                history_spacing=history_spacing,
                last_scan_offset=last_scan_offset,
                missed_offset=missed_offset,
                missed_seen_offset=missed_seen_offset,
            )
        else:
            self.volumes = build_volumes()
        # 预生成全部影像内容（总量约 1.1 MB）
        self.images: dict[tuple[str, int], bytes] = {}
        for vol in self.volumes.values():
            for ver in vol.versions:
                self.images[(vol.volume_id, ver.version)] = _make_image(ver.seed, ver.size)
        # 故障注入计数（按 卷宗+版本）
        self._fail_counts: dict[tuple[str, int], int] = {}
        self._fail_lock = __import__("threading").Lock()

    def site_time(self, real_now: float, real_start: float) -> int:
        elapsed = int((real_now - real_start) * self.rate)
        base = self._start_site_time if self._start_site_time is not None \
            else VIRTUAL_EPOCH
        return base + elapsed

    def catalog_entries(self, site_time: int) -> list[Volume]:
        """当前站点时间可见的卷宗，按当前版本更新时间倒序。"""
        visible = [v for v in self.volumes.values() if v.is_visible(site_time)]
        visible.sort(key=lambda v: v.version_at(site_time).updated_at, reverse=True)
        return visible

    def consume_failure(self, volume_id: str, version: int,
                        sequence: tuple[int, ...]) -> int | None:
        """线程安全地消费一个一次性故障状态码；没有则返回 None。"""
        if not sequence:
            return None
        key = (volume_id, version)
        with self._fail_lock:
            idx = self._fail_counts.get(key, 0)
            self._fail_counts[key] = idx + 1
        if idx < len(sequence):
            return sequence[idx]
        return None
