"""影像分段下载器。

特性：
- Range 分段读取，每个区段拿到后立即 fsync 落盘（进程被 kill -9 也保留）；
- 续采时按“区段文件名 + 期望长度”校验并复用中断前已持久化的有效区段，
  未完成的部分只补缺档，不重复拉取；
- 全部区段到齐后流式拼接并计算 sha256，校验通过才原子替换为完整影像；
- 同一卷宗的不同版本使用独立区段目录与最终文件名，互不覆盖；
- 同一 (卷宗, 版本) 的在途任务由 scanner 去重，保证只拉一份完整影像。
"""
from __future__ import annotations

import hashlib
import http.client
import logging
import os
import shutil
from pathlib import Path

from .config import Config
from .diagnose import (
    KIND_CHUNK, KIND_IMAGE, KIND_PROBE, RequestTracker,
    RESULT_FAILED, RESULT_SERVED,
)
from .http import Client, PermanentHTTPError, TransientHTTPError
from .manifest import Manifest
from .parsing import VolumeDetail

log = logging.getLogger(__name__)


class ChecksumMismatch(TransientHTTPError):
    """拼接完成后内容校验值不匹配——按临时故障重试。"""


class ImageDownloader:
    def __init__(self, cfg: Config, client: Client, manifest: Manifest,
                 tracker: RequestTracker | None = None) -> None:
        self.cfg = cfg
        self.client = client
        self.manifest = manifest
        self.tracker = tracker

    # ------------------------------------------------------------------ #
    def _volume_dir(self, vid: str) -> Path:
        d = self.cfg.images_dir / vid
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _final_path(self, detail: VolumeDetail) -> Path:
        tag = f"v{detail.version}_{detail.checksum[:12]}"
        return self._volume_dir(detail.volume_id) / f"{tag}.img"

    def _chunks_dir(self, detail: VolumeDetail) -> Path:
        tag = f"v{detail.version}_{detail.checksum[:12]}"
        return self._volume_dir(detail.volume_id) / "chunks" / tag

    def _cleanup_stale_chunk_dirs(self, detail: VolumeDetail) -> None:
        """删除同一卷宗下属于其他版本/校验值的残留区段目录。"""
        chunks_root = self._volume_dir(detail.volume_id) / "chunks"
        if not chunks_root.exists():
            return
        keep = self._chunks_dir(detail).name
        for child in chunks_root.iterdir():
            if child.is_dir() and child.name != keep:
                shutil.rmtree(child, ignore_errors=True)
                log.info("清理过期区段目录 %s", child)

    # ------------------------------------------------------------------ #
    def download(self, detail: VolumeDetail, round_no: int = 0) -> Path:
        """下载并校验，返回完整影像路径。失败抛 TransientHTTPError。"""
        vid = detail.volume_id
        final = self._final_path(detail)

        # 1) 完整影像已存在且校验通过：直接复用（重复链接 / 重跑 / 续采）
        existing = self.manifest.has_good_image(vid, detail.checksum)
        if existing and Path(existing).resolve() == final.resolve() and final.exists():
            if self._verify_final(final, detail.checksum):
                log.info("卷宗 %s v%s 完整影像已存在，直接复用", vid, detail.version)
                self.manifest.commit_image(detail, final, reused=True)
                if self.tracker is not None:
                    # 整卷复用同样留一条请求级记录：未发任何 HTTP 请求
                    self.tracker.record_reused(
                        kind=KIND_IMAGE, target=detail.image_href,
                        round_no=round_no, volume_id=vid,
                        version=detail.version, updated_at=detail.updated_at,
                        chunk_path=str(final),
                    )
                return final

        # 2) 探测总长度，与详情页声明交叉验证
        total = self._probe_size(detail, round_no)
        if total != detail.size:
            log.warning(
                "卷宗 %s 详情声明大小 %d 与实际 %d 不一致，按实际大小重新下载",
                vid, detail.size, total,
            )
            detail = VolumeDetail(
                volume_id=detail.volume_id,
                updated_at=detail.updated_at,
                checksum=detail.checksum,
                image_href=detail.image_href,
                size=total,
                version=detail.version,
            )

        self._cleanup_stale_chunk_dirs(detail)
        chunks_dir = self._chunks_dir(detail)
        chunks_dir.mkdir(parents=True, exist_ok=True)

        chunk_size = self.cfg.chunk_size
        boundaries = list(range(0, total, chunk_size))
        reused_count = 0
        fetched_count = 0

        # 3) 逐段下载；已落盘且长度正确的区段直接复用
        for index, start in enumerate(boundaries):
            end = min(start + chunk_size, total)
            chunk_path = chunks_dir / f"chunk_{index:06d}"
            expected_len = end - start

            if chunk_path.exists() and chunk_path.stat().st_size == expected_len:
                reused_count += 1
                self._record_reused_chunk(
                    detail, round_no, index, start, end,
                    chunk_path, expected_len,
                )
                continue

            self._fetch_one_chunk(detail, chunk_path, start, end, index, round_no)
            fetched_count += 1

        log.info(
            "卷宗 %s v%s 区段齐了（复用 %d 段，新拉 %d 段），开始拼接校验",
            vid, detail.version, reused_count, fetched_count,
        )

        # 4) 流式拼接 + sha256，通过后原子落地完整影像
        assembled = final.with_suffix(".img.assembling")
        digest = hashlib.sha256()
        try:
            with open(assembled, "wb") as out:
                for index in range(len(boundaries)):
                    chunk_path = chunks_dir / f"chunk_{index:06d}"
                    with open(chunk_path, "rb") as src:
                        for block in iter(lambda: src.read(1024 * 1024), b""):
                            out.write(block)
                            digest.update(block)
                    out.flush()
                os.fsync(out.fileno())
            actual = digest.hexdigest()
            if actual != detail.checksum:
                raise ChecksumMismatch(
                    f"卷宗 {vid} 校验失败: 期望 {detail.checksum} 实际 {actual}"
                )
            os.replace(assembled, final)
        finally:
            if assembled.exists():
                assembled.unlink()

        # 5) 提交清单（含单调保护），随后清理区段目录
        self.manifest.round_no = round_no
        result = self.manifest.commit_image(detail, final)
        shutil.rmtree(chunks_dir, ignore_errors=True)
        log.info(
            "卷宗 %s v%s 影像校验通过并提交清单（%s）: %s",
            vid, detail.version, result, final,
        )
        return final

    # ------------------------------------------------------------------ #
    def _record_reused_chunk(
        self, detail: VolumeDetail, round_no: int, index: int,
        start: int, end: int, chunk_path: Path, expected_len: int,
    ) -> None:
        if self.tracker is None:
            return
        self.tracker.record_reused(
            kind=KIND_CHUNK, target=detail.image_href,
            round_no=round_no, volume_id=detail.volume_id,
            version=detail.version, updated_at=detail.updated_at,
            range=[start, end], chunk_index=index,
            chunk_path=str(chunk_path), expected_len=expected_len,
        )

    def _fetch_one_chunk(
        self, detail: VolumeDetail, chunk_path: Path,
        start: int, end: int, index: int, round_no: int = 0,
    ) -> None:
        url = detail.image_href
        tmp = chunk_path.with_suffix(".part")
        # Range 头是闭区间，而内部 end 为排他上界，需减 1
        range_header = {"Range": f"bytes={start}-{end - 1}"}
        req_id = None
        if self.tracker is not None:
            req_id = self.tracker.begin(
                kind=KIND_CHUNK, target=url, round_no=round_no,
                volume_id=detail.volume_id, version=detail.version,
                updated_at=detail.updated_at,
                range_start=start, range_end=end - 1,
                chunk_index=index, chunk_path=str(chunk_path),
                expected_len=end - start,
            )
        try:
            resp = self.client.get(url, extra_headers=range_header, req_id=req_id)
        except PermanentHTTPError as exc:
            if req_id is not None:
                self.tracker.end(
                    req_id, RESULT_FAILED, site_time=getattr(exc, "site_time", None),
                    volume_id=detail.volume_id, version=detail.version,
                    updated_at=detail.updated_at, error=f"HTTP {exc.status}",
                )
            if exc.status == 404:
                # 影像尚未开放（补登窗口内）——交给上层补齐轮次
                raise TransientHTTPError(
                    f"卷宗 {detail.volume_id} 影像暂不可用(404)，等待补登"
                ) from exc
            raise
        except TransientHTTPError as exc:
            if req_id is not None:
                self.tracker.end(
                    req_id, RESULT_FAILED,
                    volume_id=detail.volume_id, version=detail.version,
                    updated_at=detail.updated_at, error=str(exc),
                )
            raise

        if resp.status == 206:
            payload = resp.data
            if len(payload) != end - start:
                err = (f"卷宗 {detail.volume_id} 区段 {index} 长度异常: "
                       f"{len(payload)} != {end - start}")
                if req_id is not None:
                    self.tracker.end(
                        req_id, RESULT_FAILED, site_time=resp.site_time,
                        volume_id=detail.volume_id, version=detail.version,
                        updated_at=detail.updated_at, error=err,
                    )
                raise TransientHTTPError(err)
        elif resp.status == 200:
            # 服务端忽略了 Range，返回整段：仅当从头开始时可整体采用
            if start != 0 or len(resp.data) != detail.size:
                err = f"卷宗 {detail.volume_id} 服务端不支持 Range 且响应不可用"
                if req_id is not None:
                    self.tracker.end(
                        req_id, RESULT_FAILED, site_time=resp.site_time,
                        volume_id=detail.volume_id, version=detail.version,
                        updated_at=detail.updated_at, error=err,
                    )
                raise TransientHTTPError(err)
            payload = resp.data
        else:
            err = f"卷宗 {detail.volume_id} 区段 {index} 意外状态码 {resp.status}"
            if req_id is not None:
                self.tracker.end(
                    req_id, RESULT_FAILED, site_time=resp.site_time,
                    volume_id=detail.volume_id, version=detail.version,
                    updated_at=detail.updated_at, error=err,
                )
            raise TransientHTTPError(err)

        # 立即持久化：tmp + fsync + 原子改名，kill -9 后要么没有要么完整
        with open(tmp, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, chunk_path)
        if req_id is not None:
            # 结果确证：HTTP 成功且区段已原子落盘
            self.tracker.end(
                req_id, RESULT_SERVED, site_time=resp.site_time,
                volume_id=detail.volume_id, version=detail.version,
                updated_at=detail.updated_at,
            )

    def _probe_size(self, detail: VolumeDetail, round_no: int = 0) -> int:
        req_id = None
        if self.tracker is not None:
            req_id = self.tracker.begin(
                kind=KIND_PROBE, target=detail.image_href, round_no=round_no,
                volume_id=detail.volume_id, version=detail.version,
                updated_at=detail.updated_at,
            )
        try:
            size = self.client.head_size(detail.image_href, req_id=req_id)
        except TransientHTTPError as exc:
            if req_id is not None:
                self.tracker.end(
                    req_id, RESULT_FAILED,
                    volume_id=detail.volume_id, version=detail.version,
                    updated_at=detail.updated_at, error=str(exc),
                )
            raise
        except http.client.HTTPException as exc:
            if req_id is not None:
                self.tracker.end(
                    req_id, RESULT_FAILED,
                    volume_id=detail.volume_id, version=detail.version,
                    updated_at=detail.updated_at, error=str(exc),
                )
            raise TransientHTTPError(f"探测影像大小失败: {exc}") from exc
        if req_id is not None:
            self.tracker.end(
                req_id, RESULT_SERVED,
                volume_id=detail.volume_id, version=detail.version,
                updated_at=detail.updated_at,
                extra={"declared_size": size},
            )
        return size

    @staticmethod
    def _verify_final(path: Path, checksum: str) -> bool:
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest() == checksum
