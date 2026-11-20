"""采集运行配置。"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    """全部可调参数；命令行参数覆盖默认值。"""

    base_url: str
    workdir: Path
    # 并发 worker 数（所有 worker 共用同一个限流器）
    workers: int = 4
    # 全站统一的最小请求发起间隔（秒，以请求头实际发出时刻为准）
    min_interval: float = 0.05
    # 建连阶段超时（秒）；建连慢不会占用限流器队首
    connect_timeout: float = 5.0
    # 请求头发出后等待响应头 / 读取响应体的超时（秒）
    read_timeout: float = 30.0
    # 单次请求的最大重试次数（临时故障 / 5xx / 503）
    max_retries: int = 6
    backoff_base: float = 0.2
    backoff_cap: float = 5.0
    # 主扫描结束后的补齐轮数（失败卷宗、补登复查）
    retry_rounds: int = 4
    # 每轮补齐之间的等待秒数
    retry_round_interval: float = 1.0
    # 分段下载的区段大小
    chunk_size: int = 64 * 1024
    # 站点口径的补登窗口（秒）：条目最迟在更新时间后 10 分钟补登
    backfill_seconds: int = 600
    user_agent: str = "aerial-crawler/1.0"

    @property
    def images_dir(self) -> Path:
        return self.workdir / "images"

    @property
    def manifest_dir(self) -> Path:
        return self.workdir / "manifest"

    @property
    def state_dir(self) -> Path:
        return self.workdir / "state"

    @property
    def log_dir(self) -> Path:
        return self.workdir / "logs"
