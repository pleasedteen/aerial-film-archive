"""解析模拟站返回的 HTML（目录页、详情页）。

不引入第三方解析库，使用标准库 html.parser，按固定的 class / 属性取值。
"""
from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Optional


@dataclass
class CatalogEntry:
    """目录页上的一个卷宗链接（同一卷可能在目录中重复出现）。"""
    volume_id: str
    href: str
    updated_at: int


@dataclass
class CatalogPage:
    entries: list[CatalogEntry]
    next_page: Optional[str]  # 下一页相对地址；末页为 None


@dataclass
class VolumeDetail:
    volume_id: str
    updated_at: int       # 按“秒”计的版本更新时间（站点时钟）
    checksum: str         # 内容校验值（sha256 hex）
    image_href: str
    size: int
    version: int


class _CatalogParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.entries: list[CatalogEntry] = []
        self.next_page: Optional[str] = None
        self._in_entry_a = False
        self._pending_href: Optional[str] = None
        self._pending_id: Optional[str] = None
        self._pending_updated: Optional[int] = None
        self._in_next_a = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attr = dict(attrs)
        classes = (attr.get("class") or "").split()
        if tag == "a" and "entry-link" in classes:
            self._in_entry_a = True
            self._pending_href = attr.get("href")
            self._pending_id = attr.get("data-volume-id")
            raw = attr.get("data-updated-at")
            self._pending_updated = int(raw) if raw and raw.isdigit() else None
        elif tag == "a" and "next-page" in classes:
            self._in_next_a = True
            self.next_page = attr.get("href")

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            if self._in_entry_a:
                self._in_entry_a = False
                if self._pending_href and self._pending_id and self._pending_updated is not None:
                    self.entries.append(
                        CatalogEntry(
                            volume_id=self._pending_id,
                            href=self._pending_href,
                            updated_at=self._pending_updated,
                        )
                    )
                self._pending_href = None
                self._pending_id = None
                self._pending_updated = None
            self._in_next_a = False


class _DetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fields: dict[str, str] = {}
        self.image_href: Optional[str] = None
        self._capture: Optional[str] = None
        self._buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attr = dict(attrs)
        classes = (attr.get("class") or "").split()
        field = next((c for c in classes if c.startswith("field-")), None)
        if field:
            self._capture = field.removeprefix("field-")
            self._buf = []
        if tag == "a" and "image-link" in classes:
            self.image_href = attr.get("href")

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._buf.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._capture:
            self.fields[self._capture] = "".join(self._buf).strip()
            self._capture = None
            self._buf = []


def parse_catalog(html_text: str) -> CatalogPage:
    parser = _CatalogParser()
    parser.feed(html_text)
    return CatalogPage(entries=parser.entries, next_page=parser.next_page)


def parse_detail(html_text: str) -> VolumeDetail:
    parser = _DetailParser()
    parser.feed(html_text)
    f = parser.fields
    try:
        return VolumeDetail(
            volume_id=f["volume-id"],
            updated_at=int(f["updated-at"]),
            checksum=f["checksum"],
            image_href=parser.image_href or "",
            size=int(f["size"]),
            version=int(f.get("version", "1")),
        )
    except (KeyError, ValueError) as exc:
        raise ValueError(f"详情页字段缺失或格式错误: {exc}; 已解析字段={f}") from exc
