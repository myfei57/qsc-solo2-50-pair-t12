"""持久化与配置：原子写入、日志分段、配置校验。"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from rubbercure.config import (
    PlatformConfig,
    SteamLimits,
    config_to_mapping,
    default_config,
    load_config,
)
from rubbercure.errors import ConfigError, StorageError
from rubbercure.store.journal import Cursor, JsonlJournal
from rubbercure.store.jsonfile import JsonStore

ROOT = Path(__file__).resolve().parents[1]


def test_json_store_roundtrip_and_digest(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    assert store.read("missing") is None
    assert store.read("missing", {"default": True}) == {"default": True}
    digest = store.write("doc", {"b": 2, "a": 1})
    assert store.exists("doc")
    assert store.read("doc") == {"a": 1, "b": 2}
    assert store.digest("doc") == digest
    assert store.remove("doc") is True
    assert store.remove("doc") is False
    assert store.digest("doc") == ""


def test_json_store_rejects_illegal_keys(tmp_path):
    store = JsonStore(str(tmp_path / "state"))
    with pytest.raises(StorageError):
        store.path_for("dir/inner")
    with pytest.raises(StorageError):
        store.path_for("")


def test_journal_rolls_segments_and_reads_in_order(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=2)
    for index in range(5):
        journal.append({"seq": index})
    assert journal.segment_count() == 3
    records = journal.read_all()
    assert [record["seq"] for record in records] == [0, 1, 2, 3, 4]
    assert len(journal.read_segment(0)) == 2
    assert journal.read_segment(99) == []


def test_journal_rejects_non_positive_segment_limit(tmp_path):
    with pytest.raises(StorageError):
        JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=0)


def _seg_path(root, name, index):
    return Path(root) / name / f"{index:05d}.jsonl"


def test_journal_resumes_from_cursor_across_segments(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=2)
    for seq in range(5):
        journal.append({"seq": seq})  # 0/1 在段0，2/3 在段1，4 在段2

    first = journal.read_after(limit=2)
    assert [record["seq"] for record in first.records] == [0, 1]
    assert first.bad_segments == []

    second = journal.read_after(first.cursor, limit=2)
    assert [record["seq"] for record in second.records] == [2, 3]

    third = journal.read_after(second.cursor)
    assert [record["seq"] for record in third.records] == [4]
    # 读到末尾后再读，应立即返回空且游标不动。
    again = journal.read_after(third.cursor)
    assert again.records == []
    assert again.cursor == third.cursor


def test_journal_cursor_picks_up_new_appends_to_active_segment(tmp_path):
    root = str(tmp_path / "state")
    journal = JsonlJournal(root, "audit", segment_limit=10)
    journal.append({"seq": 0})
    result = journal.read_after()
    assert [record["seq"] for record in result.records] == [0]

    # 游标停在活动段段尾（而非跳到下一段），之后同段追加仍能续到。
    journal.append({"seq": 1})
    continued = journal.read_after(result.cursor)
    assert [record["seq"] for record in continued.records] == [1]


def test_journal_skips_and_marks_bad_segment_but_reads_others(tmp_path):
    root = str(tmp_path / "state")
    name = "audit"
    journal = JsonlJournal(root, name, segment_limit=2)
    for seq in range(6):
        journal.append({"seq": seq})  # 段0、段1 满，段2 有 seq=4,5

    # 写坏中间段（段1）的第二行。
    bad_path = _seg_path(root, name, 1)
    lines = bad_path.read_text(encoding="utf-8").splitlines()
    lines[1] = "{这不是合法的 json"
    bad_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    journal = JsonlJournal(root, name, segment_limit=2)
    result = journal.read_after()
    # 段1 第二行坏：同段第一行(seq=2)与段0、段2 照常读出，只丢坏行。
    assert [record["seq"] for record in result.records] == [0, 1, 2, 4, 5]
    assert len(result.bad_segments) == 1
    bad = result.bad_segments[0]
    assert bad.index == 1
    assert bad.line == 2
    assert bad.as_dict() == {"segment": 1, "line": 2, "reason": bad.reason}

    # 游标已按物理行越过坏行，下次续读不会再报同一坏行。
    assert result.cursor.segment == 2
    journal.append({"seq": 6})
    nxt = journal.read_after(result.cursor)
    assert [record["seq"] for record in nxt.records] == [6]
    assert nxt.bad_segments == []


def test_journal_read_all_tolerates_bad_segment(tmp_path):
    root = str(tmp_path / "state")
    name = "audit"
    journal = JsonlJournal(root, name, segment_limit=2)
    for seq in range(4):
        journal.append({"seq": seq})
    bad_path = _seg_path(root, name, 1)
    bad_path.write_text(bad_path.read_text(encoding="utf-8") + "garbage\n", encoding="utf-8")

    journal = JsonlJournal(root, name, segment_limit=2)
    # 坏行追加在段1 尾部：坏行丢掉，同段前面的好记录全部保留。
    assert [record["seq"] for record in journal.read_all()] == [0, 1, 2, 3]


def test_journal_cursor_roundtrip_and_validation(tmp_path):
    journal = JsonlJournal(str(tmp_path / "state"), "audit", segment_limit=2)
    journal.append({"seq": 0})
    result = journal.read_after()
    restored = Cursor.from_dict(result.cursor.to_dict())
    assert restored == result.cursor

    with pytest.raises(StorageError):
        journal.read_after(Cursor(-2, 0))
    with pytest.raises(StorageError):
        journal.read_after(limit=0)


def test_load_config_from_repository_file():
    config = load_config(str(ROOT / "config" / "rubbercure.yaml"))
    assert config.name == "RubberCure"
    assert config.press.fleet_size == 3
    assert config.steam.min_pressure_kpa < config.steam.target_pressure_kpa
    mapping = config_to_mapping(config)
    assert mapping["limits"]["steam"]["target_pressure_kpa"] == config.steam.target_pressure_kpa
    assert PlatformConfig.from_mapping(mapping).name == config.name


def test_load_config_reports_missing_file(tmp_path):
    with pytest.raises(ConfigError):
        load_config(str(tmp_path / "absent.yaml"))


def test_config_rejects_missing_section_and_bad_window(tmp_path):
    with pytest.raises(ConfigError):
        PlatformConfig.from_mapping({})
    with pytest.raises(ConfigError):
        SteamLimits.from_mapping(
            {
                "target_pressure_kpa": 700.0,
                "min_pressure_kpa": 80.0,
                "max_pressure_kpa": 500.0,
                "safe_residual_kpa": 12.0,
                "purge_hold_s": 20.0,
                "purge_token_max_age_s": 180.0,
                "vent_hold_s": 8.0,
                "valve_rated_flow_kg_h": 960.0,
                "trap_rated_flow_kg_h": 240.0,
            }
        )


def test_default_config_storage_root_is_relative():
    config = default_config()
    assert not os.path.isabs(config.storage_root)
    assert config.steam.clamp_pressure(10_000.0) == config.steam.max_pressure_kpa
    assert config.steam.clamp_pressure(-5.0) == config.steam.min_pressure_kpa
