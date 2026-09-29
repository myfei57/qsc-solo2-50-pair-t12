"""文件型持久化。"""

from .jsonfile import JsonStore
from .journal import BadSegment, Cursor, JsonlJournal, ReadResult

__all__ = ["JsonStore", "JsonlJournal", "Cursor", "ReadResult", "BadSegment"]
