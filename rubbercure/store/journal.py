"""追加型 JSONL 日志，带段切换、断点续读和坏段标记。

生产审计与告警按时间顺序追加，读取方按段内行号记录游标。段达到容量阈值后
新建段，旧段不再追加；续读时从游标段和行号开始，不再从头扫描全部历史。
读取中发现坏段会写入同名 ``.corrupt`` 标记，随后跳过该段并继续读后续段，
避免一段损坏导致整批日志不可回放。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from dataclasses import dataclass

from ..errors import StorageError
from .jsonfile import JsonStore


@dataclass(frozen=True)
class JournalCursor:
    """下一条待读记录的位置：``segment`` 段内第 ``offset`` 行。"""

    segment: int = 0
    offset: int = 0

    @classmethod
    def from_mapping(cls, payload: dict) -> "JournalCursor":
        try:
            cursor = cls(int(payload["segment"]), int(payload["offset"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError(f"非法日志游标: {payload!r}", component="store") from exc
        if cursor.segment < 0 or cursor.offset < 0:
            raise StorageError(f"非法日志游标: {payload!r}", component="store")
        return cursor

    def as_dict(self) -> dict:
        return {"segment": self.segment, "offset": self.offset}


@dataclass(frozen=True)
class CorruptSegment:
    """损坏段标记。"""

    segment: int
    line: int
    error: str
    detected_at: float
    path: str

    @classmethod
    def from_mapping(cls, segment: int, payload: dict, path: str) -> "CorruptSegment":
        return cls(
            segment=segment,
            line=int(payload.get("line", 0)),
            error=str(payload.get("error", "未知错误")),
            detected_at=float(payload.get("detected_at", 0.0)),
            path=path,
        )

    def as_dict(self) -> dict:
        return {
            "segment": self.segment,
            "line": self.line,
            "error": self.error,
            "detected_at": round(self.detected_at, 3),
            "path": self.path,
        }


@dataclass(frozen=True)
class JournalReadResult:
    """一次续读的结果。"""

    records: list[dict]
    next_cursor: JournalCursor
    corruptions: list[CorruptSegment]

    def as_dict(self) -> dict:
        return {
            "entries": self.records,
            "next_cursor": self.next_cursor.as_dict(),
            "corrupt_segments": [item.as_dict() for item in self.corruptions],
        }


class JsonlJournal:
    """按段追加的 JSONL 日志。"""

    def __init__(self, root: str, name: str, segment_limit: int = 512) -> None:
        if segment_limit < 1:
            raise StorageError("段容量必须为正", component="store")
        self._root = os.path.abspath(os.path.join(root, name))
        self._segment_limit = int(segment_limit)
        self._counts: dict[int, int] = {}
        self._lock = threading.RLock()
        os.makedirs(self._root, exist_ok=True)
        self._checkpoints = JsonStore(os.path.join(self._root, ".checkpoints"))
        self._index_existing()

    def _index_existing(self) -> None:
        with self._lock:
            indexes = self._discover_segments()
            for index in indexes:
                self._counts[index] = 0
            if indexes:
                active = indexes[-1]
                self._counts[active] = self._count_lines(active)

    def _discover_segments(self) -> list[int]:
        indexes: list[int] = []
        with os.scandir(self._root) as entries:
            for entry in entries:
                if not entry.is_file() or not entry.name.endswith(".jsonl"):
                    continue
                try:
                    indexes.append(int(entry.name.removesuffix(".jsonl")))
                except ValueError:
                    continue
        return sorted(indexes)

    def _path_for(self, index: int) -> str:
        return os.path.join(self._root, f"{index:05d}.jsonl")

    def _marker_path(self, index: int) -> str:
        return self._path_for(index) + ".corrupt"

    def _count_lines(self, index: int) -> int:
        path = self._path_for(index)
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return sum(1 for line in handle if line.strip())
        except OSError:
            return 0

    def _active_segment(self) -> int:
        if not self._counts:
            self._counts[0] = 0
            return 0
        index = max(self._counts)
        while self._counts.get(index, 0) >= self._segment_limit or os.path.isfile(
            self._marker_path(index)
        ):
            index += 1
            self._counts.setdefault(index, 0)
        return index

    def append(self, record: dict) -> int:
        """追加一条记录，返回它落到的段号。"""

        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock:
            index = self._active_segment()
            target = self._path_for(index)
            try:
                with open(target, "a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:  # pragma: no cover - 依赖真实磁盘故障
                raise StorageError(f"追加审计日志失败: {exc}", component="store") from exc
            self._counts[index] = self._counts.get(index, 0) + 1
            return index

    def save_cursor(self, consumer: str, cursor: JournalCursor) -> None:
        """持久化某个读取方已处理到的游标。"""

        if not consumer:
            raise StorageError("读取方名称不能为空", component="store")
        self._checkpoints.write(f"checkpoint-{consumer}", cursor.as_dict())

    def load_cursor(self, consumer: str) -> JournalCursor | None:
        """读取某个读取方上次提交的游标；从未提交时返回 ``None``。"""

        if not consumer:
            raise StorageError("读取方名称不能为空", component="store")
        payload = self._checkpoints.read(f"checkpoint-{consumer}")
        return JournalCursor.from_mapping(payload) if payload is not None else None

    def read_resuming(
        self,
        consumer: str,
        limit: int = 500,
        commit: bool = False,
    ) -> JournalReadResult:
        """按读取方名称加载游标并续读；``commit`` 为真时立即保存新游标。"""

        result = self.read_after(self.load_cursor(consumer), limit=limit)
        if commit:
            self.save_cursor(consumer, result.next_cursor)
        return result

    def segment_count(self) -> int:
        with self._lock:
            return len(self._discover_segments())

    def corrupt_segments(self) -> list[CorruptSegment]:
        """返回已经标出的坏段。"""

        result: list[CorruptSegment] = []
        with self._lock:
            for index in self._discover_segments():
                marker = self._marker_path(index)
                if not os.path.isfile(marker):
                    continue
                try:
                    with open(marker, "r", encoding="utf-8") as handle:
                        payload = json.load(handle)
                except (OSError, ValueError):
                    payload = {"error": "坏段标记无法读取"}
                result.append(CorruptSegment.from_mapping(index, payload, marker))
        return sorted(result, key=lambda item: item.segment)

    def _mark_corrupt(self, index: int, line: int, error: Exception) -> CorruptSegment:
        marker = self._marker_path(index)
        corruption = CorruptSegment(
            segment=index,
            line=line,
            error=str(error),
            detected_at=time.time(),
            path=marker,
        )
        descriptor = None
        try:
            descriptor = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._root,
                prefix=f".{index:05d}.",
                suffix=".tmp",
                delete=False,
            )
            json.dump(
                corruption.as_dict(),
                descriptor,
                ensure_ascii=False,
                sort_keys=True,
            )
            descriptor.write("\n")
            descriptor.flush()
            os.fsync(descriptor.fileno())
            descriptor.close()
            os.replace(descriptor.name, marker)
        except OSError as exc:
            if descriptor is not None and not descriptor.closed:
                descriptor.close()
            try:
                if descriptor is not None:
                    os.unlink(descriptor.name)
            except OSError:
                pass
            raise StorageError(f"标记坏段 {index} 失败: {exc}", component="store") from exc
        return corruption

    def _load_marker(self, index: int) -> CorruptSegment:
        marker = self._marker_path(index)
        try:
            with open(marker, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError):
            payload = {"error": "坏段标记无法读取"}
        return CorruptSegment.from_mapping(index, payload, marker)

    def _read_segment(
        self,
        index: int,
        start_offset: int,
        remaining: int | None,
    ) -> tuple[list[dict], int, CorruptSegment | None, bool]:
        """读取单段，返回 ``(记录, 下一偏移, 坏段信息, 是否达到条数上限)``。"""

        records: list[dict] = []
        next_offset = start_offset
        marker = self._marker_path(index)
        if os.path.isfile(marker):
            return records, start_offset, self._load_marker(index), False

        try:
            with open(self._path_for(index), "r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle):
                    if line_number < start_offset:
                        continue
                    if not line.strip():
                        next_offset = line_number + 1
                        continue
                    try:
                        record = json.loads(line)
                    except ValueError as exc:
                        marked = self._mark_corrupt(index, line_number, exc)
                        return records, next_offset, marked, False
                    if not isinstance(record, dict):
                        error = ValueError("日志记录必须是 JSON 对象")
                        marked = self._mark_corrupt(index, line_number, error)
                        return records, next_offset, marked, False
                    next_offset = line_number + 1
                    records.append(record)
                    if remaining is not None and len(records) >= remaining:
                        return records, next_offset, None, True
        except OSError as exc:
            marked = self._mark_corrupt(index, next_offset, exc)
            return records, next_offset, marked, False
        return records, next_offset, None, False

    def read_after(
        self,
        cursor: JournalCursor | None = None,
        limit: int = 500,
    ) -> JournalReadResult:
        """从游标位置续读至多 ``limit`` 条有效记录。

        已损坏或读取时发现损坏的段只进入 ``corruptions``，不会中断后续段的
        读取。返回的 ``next_cursor`` 是下一条尚未处理的位置。
        """

        if cursor is None:
            cursor = JournalCursor()
        if limit < 1:
            raise StorageError("读取条数必须为正", component="store")

        records: list[dict] = []
        corruptions: list[CorruptSegment] = []
        current = cursor
        with self._lock:
            for index in self._discover_segments():
                if index < current.segment:
                    continue
                start_offset = current.offset if index == current.segment else 0
                segment_records, next_offset, corruption, reached_limit = (
                    self._read_segment(
                        index,
                        start_offset,
                        limit - len(records),
                    )
                )
                records.extend(segment_records if corruption is None else [])
                if corruption is not None:
                    corruptions.append(corruption)
                    current = JournalCursor(index + 1, 0)
                elif reached_limit:
                    current = JournalCursor(index, next_offset)
                    return JournalReadResult(records, current, corruptions)
                else:
                    current = JournalCursor(index, next_offset)

        return JournalReadResult(records, current, corruptions)

    def read_all(self) -> list[dict]:
        """按段号顺序读回全部有效记录；坏段跳过。"""

        records: list[dict] = []
        cursor = JournalCursor()
        while True:
            result = self.read_after(cursor, limit=1000)
            records.extend(result.records)
            if result.next_cursor == cursor:
                return records
            cursor = result.next_cursor

    def read_segment(self, index: int) -> list[dict]:
        """读回指定段；段损坏或不存在时返回空列表，坏段可用专用接口查询。"""

        if index < 0 or not os.path.isfile(self._path_for(index)):
            return []
        with self._lock:
            records, _offset, corruption, _limit = self._read_segment(
                index, 0, None
            )
        return [] if corruption is not None else records
