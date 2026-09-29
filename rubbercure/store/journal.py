"""追加型 JSONL 日志，带段切换。

生产审计与告警按时间顺序追加，读取方必须能连续读回整段历史。段达到容量
阈值后新建段，旧段不再追加，读取时按段序拼接，保证顺序与写入顺序一致。

读取侧支持三件事：

- 断点续读：游标 = (段号, 段内已读物理行数)，``read_after`` 从游标位置
  接着读，不必每次从头整段捞；支持 ``limit`` 分批，返回的新游标可直接
  持久化后下次再用；
- 坏段隔离：某一段打不开、或其中某一行 JSON 损坏，只把坏掉的行（连同
  所在段号）登记进 ``bad_segments`` 并跳过，同段其余好行与其他段照常
  读出；游标按物理行前移，不会反复卡在同一个坏行上；
- 顺序不变：始终按段号、段内行序读出，与写入顺序一致。
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field

from ..errors import StorageError


@dataclass(frozen=True)
class Cursor:
    """读取位置：``segment`` 段内已消费 ``line`` 个物理行。

    ``line`` 按物理行（含空行）计，而非好记录数，这样段内即便混入坏行，
    续读位置依然稳定不回退。``segment == -1`` 表示从头开始。
    """

    segment: int = -1
    line: int = 0

    @classmethod
    def start(cls) -> "Cursor":
        """日志起点（第 0 段第 0 行之前）。"""

        return cls(-1, 0)

    def to_dict(self) -> dict:
        """可直接 JSON 持久化的形式。"""

        return {"segment": self.segment, "line": self.line}

    @classmethod
    def from_dict(cls, data: dict) -> "Cursor":
        """从持久化的字典恢复游标。"""

        return cls(int(data["segment"]), int(data.get("line", 0)))


@dataclass(frozen=True)
class BadSegment:
    """坏段标记：``line`` 为段内首个坏行的行号（从 1 起），0 表示整段打不开。"""

    index: int
    line: int
    reason: str

    def as_dict(self) -> dict:
        return {"segment": self.index, "line": self.line, "reason": self.reason}


@dataclass(frozen=True)
class ReadResult:
    """一次续读的结果：好记录、下次续读游标、本次发现的坏段。"""

    records: list[dict]
    cursor: Cursor
    bad_segments: list[BadSegment] = field(default_factory=list)


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
        self._index_existing()

    def _index_existing(self) -> None:
        with self._lock:
            for entry in os.listdir(self._root):
                if not entry.endswith(".jsonl"):
                    continue
                try:
                    index = int(entry.split(".")[0])
                except ValueError:
                    continue
                with open(os.path.join(self._root, entry), "r", encoding="utf-8") as handle:
                    self._counts[index] = sum(1 for _ in handle)

    def _segment_indexes(self) -> list[int]:
        """磁盘上现存的全部段号（并上内存计数），升序。

        每次读取都现扫目录，而不是只认启动时的计数：段文件可能在运行期
        被外部清理或补入。
        """

        found = set(self._counts)
        for entry in os.listdir(self._root):
            if not entry.endswith(".jsonl"):
                continue
            try:
                found.add(int(entry.split(".")[0]))
            except ValueError:
                continue
        return sorted(found)

    def _active_segment(self) -> int:
        if not self._counts:
            self._counts[0] = 0
            return 0
        index = max(self._counts)
        if self._counts[index] >= self._segment_limit:
            index += 1
            self._counts.setdefault(index, 0)
        return index

    def append(self, record: dict) -> int:
        """追加一条记录，返回它落到的段号。"""

        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock:
            index = self._active_segment()
            target = os.path.join(self._root, f"{index:05d}.jsonl")
            try:
                with open(target, "a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:  # pragma: no cover - 依赖真实磁盘故障
                raise StorageError(f"追加审计日志失败: {exc}", component="store") from exc
            self._counts[index] = self._counts.get(index, 0) + 1
            return index

    def segment_count(self) -> int:
        with self._lock:
            return len(self._counts)

    def read_after(self, cursor: Cursor | None = None, *, limit: int | None = None) -> ReadResult:
        """从 ``cursor`` 之后续读。

        ``limit`` 给定时最多返回该数量的好记录，并可能停在段中间，返回的
        游标下次继续；坏行不计入 ``limit``。坏行逐行跳过并把其段号/行号
        登记到结果里，游标按物理行前移，重复调用不会再报同一坏行。
        """

        if cursor is None:
            cursor = Cursor.start()
        if cursor.segment < -1 or cursor.line < 0:
            raise StorageError(f"非法读取游标: {cursor!r}", component="store")
        if limit is not None and limit < 1:
            raise StorageError("读取上限必须为正", component="store")

        with self._lock:
            indexes = self._segment_indexes()
            records: list[dict] = []
            bad_segments: list[BadSegment] = []
            current = cursor
            for index in indexes:
                if index < cursor.segment:
                    continue
                start_line = cursor.line if index == cursor.segment else 0
                budget = None if limit is None else limit - len(records)
                emitted, next_line, exhausted, failures = self._scan_segment(
                    index, start_line, budget
                )
                records.extend(emitted)
                for line_no, reason in failures:
                    bad_segments.append(BadSegment(index, line_no, reason))
                current = Cursor(index, next_line)
                if not exhausted:
                    # 因 limit 停在段中间，后续段本次不读。
                    break
        return ReadResult(records=records, cursor=current, bad_segments=bad_segments)

    def _scan_segment(
        self, index: int, start_line: int, budget: int | None
    ) -> tuple[list[dict], int, bool, list[tuple[int, str]]]:
        """扫描单段。

        返回 (好记录, 下次续读物理行号, 是否读到段尾, 坏点列表)。坏点为
        ``(从 1 起的行号, 原因)``，``line=0`` 表示整段打不开。坏行逐行跳过，
        同段其余好行照常返回；游标按物理行推进，坏行不会被重复上报。
        """

        path = os.path.join(self._root, f"{index:05d}.jsonl")
        if not os.path.isfile(path):
            return [], start_line, True, []
        try:
            handle = open(path, "r", encoding="utf-8")
        except OSError as exc:  # pragma: no cover - 依赖真实磁盘故障
            return [], start_line, True, [(0, f"无法打开段文件: {exc}")]

        emitted: list[dict] = []
        failures: list[tuple[int, str]] = []
        next_line = start_line
        exhausted = True
        with handle:
            for physical, raw in enumerate(handle):
                if physical < start_line:
                    next_line = physical + 1
                    continue
                next_line = physical + 1
                if not raw.strip():
                    continue
                try:
                    value = json.loads(raw)
                except json.JSONDecodeError as exc:
                    failures.append((physical + 1, f"JSON 损坏: {exc.msg}"))
                    continue
                if not isinstance(value, dict):
                    failures.append((physical + 1, "记录不是 JSON 对象"))
                    continue
                emitted.append(value)
                if budget is not None and len(emitted) >= budget:
                    exhausted = False
                    break
        return emitted, next_line, exhausted, failures

    def read_all(self) -> list[dict]:
        """按段号顺序读回全部好记录（坏段自动跳过，不影响其余段）。"""

        return self.read_after().records

    def read_segment(self, index: int) -> list[dict]:
        """读回指定段。

        显式点名读取单段时不做容错：段不存在返回空列表，段内有坏行直接抛
        ``json.JSONDecodeError``，便于调用方定位问题。批量/续读请用
        :meth:`read_after`，那条路径会标记并跳过坏段。
        """

        path = os.path.join(self._root, f"{index:05d}.jsonl")
        if not os.path.isfile(path):
            return []
        with open(path, "r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
