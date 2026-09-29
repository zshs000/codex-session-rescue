"""Remove opaque encrypted history and synchronize one Codex thread's indexes.

Dry run:  python repair_encrypted_session.py THREAD_ID
Repair:   python repair_encrypted_session.py THREAD_ID --apply
Prepare:  python repair_encrypted_session.py THREAD_ID --prepare
Apply a prepared snapshot: --apply-backup BACKUP_DIRECTORY
Undo, only if the thread has not changed: --restore BACKUP_DIRECTORY

Supports fully projected, ordinal-bearing local rollouts and automatically
dispatches rollback history chains to the split-history repair backend.
Unknown formats, unrelated shared history, active writers, and stale snapshots
are refused. No network or model requests are made.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid


KEYS = {
    "thread_history_projection_state": ("thread_id",),
    "thread_turns": ("thread_id", "turn_id"),
    "thread_items": ("thread_id", "turn_id", "item_id"),
    "thread_realtime_items": ("thread_id", "item_id"),
}
ORDINAL_COLUMNS = {"rollout_ordinal", "rollout_end_ordinal", "updated_at_ordinal"}
OFFSET_COLUMNS = {"rollout_byte_offset", "rollout_end_byte_offset", "next_rollout_byte_offset"}
POSITION_COLUMNS = ORDINAL_COLUMNS | OFFSET_COLUMNS | {"next_rollout_ordinal"}
DROP = object()


class RepairError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise RepairError(message)


def normalized_path(value):
    value = str(value)
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return Path(value).resolve()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_tables(tables):
    return {name: sorted(canonical(row) for row in rows) for name, rows in tables.items()}


def write_new(path, data):
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def save_json(path, value):
    stage = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    write_new(stage, (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))
    os.replace(stage, path)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def connect_readonly(path):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def read_tables(connection, thread_id):
    return {
        table: [dict(row) for row in connection.execute(
            "SELECT * FROM " + table + " WHERE thread_id=?", (thread_id,)
        )]
        for table in KEYS
    }


def directory_metadata(home, thread_id):
    connection = connect_readonly(home / "state_5.sqlite")
    try:
        row = connection.execute("SELECT * FROM threads WHERE id=?", (thread_id,)).fetchone()
        require(row is not None, "Thread is missing from state_5.sqlite.")
        thread = dict(row)
    finally:
        connection.close()
    entries = []
    index = home / "session_index.jsonl"
    if index.exists():
        for line in index.read_bytes().splitlines():
            if line.strip():
                entry = json.loads(line)
                if entry.get("id") == thread_id:
                    entries.append(entry)
    return {"state_thread": thread, "session_index_entries": entries}


def require_unshared_history(home, thread_id, catalog_entries=None):
    if catalog_entries is not None:
        for candidates in catalog_entries.values():
            for path, metadata in candidates:
                require((metadata.get("history_base") or {}).get("thread_id") != thread_id,
                        "Another rollout uses this thread as a history base; shared history needs a separate repair.")
        return
    for folder in ("sessions", "archived_sessions"):
        for path in (home / folder).rglob("*.jsonl"):
            try:
                with path.open("rb") as stream:
                    metadata = json.loads(stream.readline()).get("payload", {})
            except (OSError, ValueError, AttributeError):
                continue
            base = metadata.get("history_base") or {}
            require(base.get("thread_id") != thread_id,
                    "Another rollout uses this thread as a history base; shared history needs a separate repair.")


@dataclass
class Rollout:
    data: bytes
    records: list
    lines: list
    offsets: list

    @classmethod
    def parse(cls, data, thread_id):
        require(data and data.endswith(b"\n"), "Rollout must end with a complete JSONL record.")
        lines = data.splitlines(keepends=True)
        records, offsets = [], [0]
        for ordinal, line in enumerate(lines):
            try:
                record = json.loads(line)
            except (ValueError, UnicodeError) as exc:
                raise RepairError(f"Invalid JSONL at line {ordinal + 1}.") from exc
            require(isinstance(record, dict), "Rollout record is not an object.")
            require(type(record.get("ordinal")) is int and record["ordinal"] == ordinal,
                    f"Non-contiguous explicit ordinal at line {ordinal + 1}; repair that first.")
            records.append(record)
            offsets.append(offsets[-1] + len(line))
        meta = records[0]
        require(meta.get("type") == "session_meta" and meta.get("payload", {}).get("id") == thread_id,
                "Rollout session identity does not match the requested thread.")
        require(not meta["payload"].get("history_base"), "Split/fork history needs a separate repair.")
        return cls(data, records, lines, offsets)


def encrypted_count(value):
    if isinstance(value, dict):
        return int(bool(value.get("encrypted_content"))) + sum(encrypted_count(v) for v in value.values())
    if isinstance(value, list):
        return sum(encrypted_count(v) for v in value)
    return 0


def strip_opaque(value, counts, visible=None):
    """Visit JSON objects only; strings containing example code remain untouched."""
    if isinstance(value, dict):
        if value.get("type") == "reasoning" and value.get("encrypted_content"):
            if value.get("summary") or value.get("content"):
                def texts(node):
                    if isinstance(node, str):
                        yield node
                    elif isinstance(node, dict):
                        for child in node.values():
                            yield from texts(child)
                    elif isinstance(node, list):
                        for child in node:
                            yield from texts(child)
                excerpts = [item.get("text", "") for item in value.get("summary", [])] + list(texts(value.get("content", [])))
                preserved = list(texts((visible or {}).get(value.get("id"), {})))
                require(all(any(excerpt in text for text in preserved) for excerpt in excerpts),
                        "Encrypted reasoning has readable content without a preserved UI copy.")
                counts["readable_summaries_preserved_in_ui"] += 1
            allowed = {"type", "id", "summary", "content", "encrypted_content",
                       "internal_chat_message_metadata_passthrough"}
            require(set(value) <= allowed, "Unknown encrypted reasoning fields; inspect before repair.")
            require(isinstance(value["encrypted_content"], str), "Unknown encrypted reasoning format.")
            counts["removed_opaque_reasoning_objects"] += 1
            return DROP
        if value.get("type") == "encrypted_content" and value.get("encrypted_content"):
            require(set(value) == {"type", "encrypted_content"}, "Encrypted block contains extra data.")
            require(isinstance(value["encrypted_content"], str), "Unknown encrypted block format.")
            counts["removed_encrypted_content_blocks"] += 1
            return DROP
        require(not value.get("encrypted_content"), "Unsupported encrypted-content location; inspect it first.")
        result = {}
        for key, item in value.items():
            cleaned = strip_opaque(item, counts, visible)
            if cleaned is DROP:
                require(key == "payload" and value.get("type") == "response_item",
                        "Opaque content appeared outside a response item or history array.")
                return DROP
            result[key] = cleaned
        if value.get("type") == "agent_message" and value.get("content"):
            require(result.get("content"), "Agent message has no readable content after cleanup.")
        return result
    if isinstance(value, list):
        result = []
        for item in value:
            cleaned = strip_opaque(item, counts, visible)
            if cleaned is not DROP:
                result.append(cleaned)
        return result
    return value


def build_candidate(original, thread_id):
    parts, ordinals, boundaries, offsets = [], {}, {}, {}
    counts = Counter()
    visible = {r["payload"]["item"]["id"]: r["payload"]["item"] for r in original.records
               if r.get("payload", {}).get("type") == "item_completed"}
    last_plaintext = max((r["ordinal"] for r in original.records if r.get("type") == "compacted"
                          and r.get("payload", {}).get("message")
                          and not encrypted_count(r["payload"].get("replacement_history"))), default=-1)
    new_offset = 0
    for old_ordinal, (raw, record) in enumerate(zip(original.lines, original.records)):
        offsets[original.offsets[old_ordinal]] = new_offset
        boundaries[old_ordinal] = len(parts)
        payload = record.get("payload", {})
        if record.get("type") == "compacted" and any(item.get("type") == "compaction" and item.get("encrypted_content") for item in (payload.get("replacement_history") or [])):
            require(not payload.get("message") and old_ordinal < last_plaintext,
                    "An active opaque compaction checkpoint cannot be discarded.")
            counts["removed_superseded_opaque_checkpoints"] += 1
            continue
        cleaned = strip_opaque(record, counts, visible)
        if cleaned is DROP:
            require(record.get("type") == "response_item" and record["payload"].get("type") == "reasoning",
                    "Unexpected top-level record removal.")
            counts["removed_rollout_records"] += 1
            continue
        ordinals[old_ordinal] = len(parts)
        cleaned["ordinal"] = len(parts)
        if cleaned["ordinal"] != record["ordinal"]:
            counts["renumbered_rollout_records"] += 1
        encoded = raw if cleaned == record else (json.dumps(cleaned, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        parts.append(encoded)
        new_offset += len(encoded)
    offsets[len(original.data)] = new_offset
    boundaries[len(original.records)] = len(parts)
    candidate = Rollout.parse(b"".join(parts), thread_id)
    require(encrypted_count(candidate.records) == 0, "Encrypted content remains in the candidate.")
    # Visible events, messages, and tool I/O are separate from opaque responses.
    def visible_records(rollout):
        return [{k: v for k, v in r.items() if k != "ordinal"} for r in rollout.records
                if r.get("type") not in {"response_item", "compacted"}
                or (r.get("type") == "response_item" and r.get("payload", {}).get("type") not in {"reasoning", "agent_message"})]
    require(visible_records(original) == visible_records(candidate), "Readable messages, events, or tool data changed.")
    return candidate, ordinals, boundaries, offsets, dict(counts)


def remap_tables(tables, ordinals, boundaries, offsets):
    result = {}
    for table, rows in tables.items():
        result[table] = []
        for row in rows:
            changed = dict(row)
            for column, value in row.items():
                if value is None:
                    continue
                mapping = (ordinals if column in ORDINAL_COLUMNS else offsets if column in OFFSET_COLUMNS
                           else boundaries if column == "next_rollout_ordinal" else None)
                if mapping is not None:
                    require(value in mapping, f"{table}.{column} references an unsupported or removed position.")
                    changed[column] = mapping[value]
            result[table].append(changed)
    return result


def validate_index(rollout, tables, thread_id):
    size = len(rollout.records)
    projection = tables["thread_history_projection_state"]
    require(len(projection) == 1, "Expected exactly one history projection cursor.")
    require((projection[0]["next_rollout_ordinal"], projection[0]["next_rollout_byte_offset"]) == (size, len(rollout.data)),
            "History projection is not at EOF; catch up the native index before using this repair.")
    for table, rows in tables.items():
        for row in rows:
            require(row["thread_id"] == thread_id, "Index contains a different thread.")
            for column in ORDINAL_COLUMNS & row.keys():
                value = row[column]
                require(value is None or type(value) is int and 0 <= value < size, f"Invalid {table}.{column}.")
            for column in OFFSET_COLUMNS & row.keys():
                require(row[column] is None or row[column] in rollout.offsets, f"Invalid {table}.{column} byte boundary.")
    for turn in tables["thread_turns"]:
        ordinal = turn["rollout_ordinal"]
        payload = rollout.records[ordinal].get("payload", {})
        require(payload.get("type") == "task_started" and payload.get("turn_id") == turn["turn_id"],
                "Turn start points at the wrong record.")
        require(turn.get("rollout_byte_offset") == rollout.offsets[ordinal], "Turn start offset is not synchronized.")
        end = turn.get("rollout_end_ordinal")
        if end is not None:
            require(turn.get("rollout_end_byte_offset") == rollout.offsets[end + 1], "Turn end offset is not synchronized.")
    for item in tables["thread_items"]:
        payload = rollout.records[item["rollout_ordinal"]].get("payload", {})
        require(payload.get("item", {}).get("id") == item["item_id"] and payload.get("turn_id") == item["turn_id"],
                "History item points at the wrong source record.")


def make_plan(home, thread_id, catalog_entries=None):
    require(str(uuid.UUID(thread_id)) == thread_id, "Use a canonical thread UUID.")
    require_unshared_history(home, thread_id, catalog_entries)
    metadata = directory_metadata(home, thread_id)
    source = normalized_path(metadata["state_thread"]["rollout_path"])
    require(source.suffix == ".jsonl" and any(source.is_relative_to(home / name) for name in ("sessions", "archived_sessions")),
            "Rollout is outside this Codex home's session directories.")
    original = Rollout.parse(source.read_bytes(), thread_id)
    connection = connect_readonly(home / "thread_history_1.sqlite")
    try:
        connection.execute("BEGIN")
        before = read_tables(connection, thread_id)
    finally:
        connection.close()
    validate_index(original, before, thread_id)
    candidate, ordinals, boundaries, offsets, counts = build_candidate(original, thread_id)
    after = remap_tables(before, ordinals, boundaries, offsets)
    validate_index(candidate, after, thread_id)
    changes = {table: sum(a != b for a, b in zip(before[table], after[table])) for table in KEYS}
    summary = {
        "session_id": thread_id, "codex_home": str(home), "source": str(source),
        "history_database": str(home / "thread_history_1.sqlite"),
        "before_sha256": digest(original.data), "after_sha256": digest(candidate.data),
        "before_bytes": len(original.data), "after_bytes": len(candidate.data),
        "before_records": len(original.records), "after_records": len(candidate.records),
        "encrypted_fields_before": encrypted_count(original.records), "encrypted_fields_after": 0,
        "transformations": counts, "index_rows_changed": changes,
        "index_row_counts": {table: len(rows) for table, rows in before.items()},
        "validation": {"contiguous_ordinals": True, "index_boundaries_valid": True,
                       "item_source_ids_match": True, "projection_at_eof": True,
                       "readable_events_messages_tools_preserved": True},
    }
    return summary, original, candidate, before, after, metadata


def prepare(home, thread_id):
    plan = make_plan(home, thread_id)
    summary, original, candidate, before, after, metadata = plan
    require(summary["encrypted_fields_before"] > 0, "This thread already has no nonempty encrypted fields.")
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    backup = home / "backups_state" / f"encrypted-repair-{thread_id}-{stamp}"
    backup.mkdir(parents=True, exist_ok=False)
    write_new(backup / "original.jsonl", original.data)
    write_new(backup / "repaired.jsonl", candidate.data)
    save_json(backup / "history-index.before.json", before)
    save_json(backup / "history-index.after.json", after)
    save_json(backup / "directory-metadata.before.json", metadata)
    manifest = {**summary, "state": "prepared", "backup": str(backup), "prepared_at": datetime.now().astimezone().isoformat()}
    save_json(backup / "manifest.json", manifest)
    return backup, manifest


@contextmanager
def thread_guard(home, thread_id):
    """Hold the native thread's lock pathname exclusively; never stop a process."""
    lock = home / "thread-writer-locks" / (thread_id + ".lock")
    lock.parent.mkdir(exist_ok=True)
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateFileW.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                      wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE)
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel.CloseHandle.restype = wintypes.BOOL
        handle = kernel.CreateFileW(str(lock), 0xC0000000, 0, None, 1, 0x04000080, None)
        require(handle != wintypes.HANDLE(-1).value,
                f"Cannot acquire target thread writer lock (Windows error {ctypes.get_last_error()}); close that thread first.")
        try:
            yield
        finally:
            kernel.CloseHandle(handle)  # DELETE_ON_CLOSE also handles unexpected process exit.
    else:
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        except FileExistsError as exc:
            raise RepairError("Target thread writer lock exists; close that thread first.") from exc
        try:
            yield
        finally:
            os.close(descriptor)
            lock.unlink()


def update_positions(connection, expected, wanted):
    for table, key_columns in KEYS.items():
        previous = {tuple(row[key] for key in key_columns): row for row in expected[table]}
        desired = {tuple(row[key] for key in key_columns): row for row in wanted[table]}
        require(previous.keys() == desired.keys(), "Repair must preserve all history index rows.")
        movements = {}
        for key, row in desired.items():
            if "rollout_ordinal" in row:
                delta = row["rollout_ordinal"] - previous[key]["rollout_ordinal"]
                if delta:
                    movements.setdefault(row["thread_id"], set()).add(1 if delta > 0 else -1)
        require(all(len(signs) == 1 for signs in movements.values()), "Mixed ordinal movement needs a separate migration.")
        # Unique (thread_id, rollout_ordinal) indexes require freeing occupied
        # positions first: ascending when shrinking, descending when restoring.
        def update_order(entry):
            key, row = entry
            old_ordinal = previous[key].get("rollout_ordinal", 0)
            increasing = movements.get(row["thread_id"]) == {1}
            return row["thread_id"], -old_ordinal if increasing else old_ordinal
        for key, row in sorted(desired.items(), key=update_order):
            changed = [column for column in row if row[column] != previous[key][column]]
            require(set(changed) <= POSITION_COLUMNS, "Repair would change indexed conversation content.")
            if changed:
                sql = "UPDATE " + table + " SET " + ",".join(column + "=?" for column in changed)
                sql += " WHERE " + " AND ".join(column + "=?" for column in key_columns)
                cursor = connection.execute(sql, [row[column] for column in changed] + list(key))
                require(cursor.rowcount == 1, "History index row changed during repair.")


def apply_backup(backup, restore=False):
    manifest = read_json(backup / "manifest.json")
    home = normalized_path(manifest["codex_home"])
    require(backup.is_relative_to(home / "backups_state"), "Backup is outside its recorded Codex home.")
    thread_id = manifest["session_id"]
    require(str(uuid.UUID(thread_id)) == thread_id, "Invalid backup thread ID.")
    source = normalized_path(manifest["source"])
    database = normalized_path(manifest["history_database"])
    require(database == home / "thread_history_1.sqlite", "Unexpected history database path.")
    require(source.suffix == ".jsonl" and any(source.is_relative_to(home / name) for name in ("sessions", "archived_sessions")),
            "Unexpected rollout path in backup.")
    expected_side, wanted_side = ("after", "before") if restore else ("before", "after")
    expected_hash, wanted_hash = (manifest[side + "_sha256"] for side in (expected_side, wanted_side))
    expected = read_json(backup / f"history-index.{expected_side}.json")
    wanted = read_json(backup / f"history-index.{wanted_side}.json")
    metadata = read_json(backup / "directory-metadata.before.json")
    wanted_data = (backup / ("original.jsonl" if restore else "repaired.jsonl")).read_bytes()
    require(digest(wanted_data) == wanted_hash, "Backup/candidate checksum does not match.")
    wanted_rollout = Rollout.parse(wanted_data, thread_id)
    validate_index(wanted_rollout, wanted, thread_id)
    stage = source.with_name(source.name + ".repair-" + uuid.uuid4().hex + ".tmp")
    with thread_guard(home, thread_id):
        require_unshared_history(home, thread_id)
        current_data = source.read_bytes()
        require(digest(current_data) == expected_hash, "Thread history changed; refusing to overwrite new messages. Prepare a fresh repair.")
        require(directory_metadata(home, thread_id) == metadata, "Thread directory metadata changed since preparation.")
        connection = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=10)
        connection.row_factory = sqlite3.Row
        replaced = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            require(canonical_tables(read_tables(connection, thread_id)) == canonical_tables(expected),
                    "Thread history index changed since preparation.")
            update_positions(connection, expected, wanted)
            require(canonical_tables(read_tables(connection, thread_id)) == canonical_tables(wanted), "Index update verification failed.")
            write_new(stage, wanted_data)
            require(digest(source.read_bytes()) == expected_hash, "Rollout changed during the transaction.")
            os.replace(stage, source)
            replaced = True
            require(digest(source.read_bytes()) == wanted_hash, "Written rollout checksum mismatch.")
            connection.commit()
        except BaseException:
            connection.rollback()
            if replaced:
                require(canonical_tables(read_tables(connection, thread_id)) == canonical_tables(expected),
                        "Commit outcome is uncertain; keep the backup and inspect before restoring.")
                require(digest(source.read_bytes()) == wanted_hash, "Rollout changed unexpectedly; automatic rollback refused.")
                write_new(stage, current_data)
                os.replace(stage, source)
            raise
        finally:
            connection.close()
            if stage.exists():
                stage.unlink()
        require(digest(source.read_bytes()) == wanted_hash, "Post-commit rollout validation failed.")
        connection = connect_readonly(database)
        try:
            actual = read_tables(connection, thread_id)
            require(canonical_tables(actual) == canonical_tables(wanted), "Post-commit index verification failed.")
            validate_index(wanted_rollout, actual, thread_id)
        finally:
            connection.close()
        require(directory_metadata(home, thread_id) == metadata, "Directory metadata changed during verification.")
        manifest["state"] = "restored" if restore else "applied"
        manifest[manifest["state"] + "_at"] = datetime.now().astimezone().isoformat()
        manifest["validation"]["live_file_and_index_verified"] = True
        manifest["validation"]["state_and_session_directory_preserved"] = True
        save_json(backup / "manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("thread_id", nargs="?")
    parser.add_argument("--codex-home", type=Path, default=Path.home() / ".codex")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--prepare", action="store_true")
    action.add_argument("--apply", action="store_true")
    action.add_argument("--apply-backup", type=Path)
    action.add_argument("--restore", type=Path)
    args = parser.parse_args()
    if args.apply_backup or args.restore:
        require(not args.thread_id, "Use a thread ID or a backup action, not both.")
        folder = normalized_path(args.apply_backup or args.restore)
        if read_json(folder / "manifest.json").get("format") == "split-v1":
            import repair_split_encrypted_session as backend
            result = backend.apply_backup(folder, restore=bool(args.restore))
        else:
            result = apply_backup(folder, restore=bool(args.restore))
    else:
        require(args.thread_id is not None, "A thread ID is required.")
        home = normalized_path(args.codex_home)
        metadata = directory_metadata(home, args.thread_id)
        source = normalized_path(metadata["state_thread"]["rollout_path"])
        with source.open("rb") as stream:
            split_history = bool(json.loads(stream.readline()).get("payload", {}).get("history_base"))
        plan_fn, prepare_fn, apply_fn = make_plan, prepare, apply_backup
        if split_history:
            import repair_split_encrypted_session as backend
            plan_fn, prepare_fn, apply_fn = backend.plan, backend.prepare, backend.apply_backup
        if args.prepare or args.apply:
            backup, result = prepare_fn(home, args.thread_id)
            if args.apply:
                result = apply_fn(backup)
        else:
            result = {"state": "dry_run", **plan_fn(home, args.thread_id)[0]}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    sys.modules.setdefault("repair_encrypted_session", sys.modules[__name__])
    sys.stdout.reconfigure(encoding="utf-8")
    try:
        main()
    except (RepairError, sqlite3.Error, OSError, ValueError) as error:
        print(f"Repair stopped: {error}", file=sys.stderr)
        raise SystemExit(1)
