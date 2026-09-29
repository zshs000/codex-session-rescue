"""Clean one rollback history chain, preserving its files, IDs and visible history."""
import argparse
from collections import Counter
from contextlib import ExitStack
from datetime import datetime
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid

import repair_encrypted_session as common

require = common.require


def physical_id(path):
    match = re.search(r"([0-9a-f-]{36})$", path.stem)
    require(match is not None, "Unrecognized rollout filename.")
    return match.group(1)


def catalog(home):
    result = {}
    for folder in ("sessions", "archived_sessions"):
        for path in (home / folder).rglob("*.jsonl"):
            try:
                with path.open("rb") as stream:
                    meta = json.loads(stream.readline())["payload"]
                identity = physical_id(path)
            except (OSError, ValueError, KeyError, common.RepairError):
                continue
            result.setdefault(identity, []).append((path.resolve(), meta))
    return result


def check_references(entries, paths, identities):
    for candidates in entries.values():
        for path, meta in candidates:
            if (meta.get("history_base") or {}).get("thread_id") in identities:
                require(path in paths, "Another thread shares this history; a private copy is required.")


def parse(data, logical_id):
    require(data.endswith(b"\n"), "Incomplete final rollout record.")
    lines = data.splitlines(keepends=True)
    records = [json.loads(raw) for raw in lines]
    start = records[0]["ordinal"]
    require([r.get("ordinal") for r in records] == list(range(start, start + len(records))), "Segment ordinals are not contiguous.")
    require(records[0]["type"] == "session_meta" and records[0]["payload"]["id"] == logical_id, "Segment belongs to another logical thread.")
    offsets = [0]
    for raw in lines:
        offsets.append(offsets[-1] + len(raw))
    return {"data": data, "lines": lines, "records": records, "start": start, "offsets": offsets}


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from strings(child)


def clean(value, visible, counts):
    if isinstance(value, dict):
        if value.get("type") == "reasoning" and value.get("encrypted_content"):
            require(isinstance(value["encrypted_content"], str), "Unknown reasoning encryption format.")
            require(set(value) <= {"type", "id", "summary", "content", "encrypted_content", "internal_chat_message_metadata_passthrough"}, "Unknown reasoning fields.")
            excerpts = [r.get("text", "") for r in value.get("summary", [])] + list(strings(value.get("content", [])))
            ui = list(strings(visible.get(value.get("id"), {})))
            require(all(any(text in existing for existing in ui) for text in excerpts), "Readable reasoning has no preserved UI copy.")
            counts["readable_summaries_preserved_in_ui"] += int(bool(excerpts))
            counts["removed_reasoning_objects"] += 1
            return common.DROP
        if value.get("type") == "encrypted_content" and value.get("encrypted_content"):
            require(set(value) == {"type", "encrypted_content"}, "Encrypted block contains additional data.")
            counts["removed_encrypted_blocks"] += 1
            return common.DROP
        require(not value.get("encrypted_content"), "Unsupported encrypted object.")
        result = {}
        for key, child in value.items():
            sanitized = clean(child, visible, counts)
            if sanitized is common.DROP:
                require(key == "payload" and value.get("type") == "response_item", "Unexpected record removal.")
                return common.DROP
            result[key] = sanitized
        return result
    if isinstance(value, list):
        result = []
        for child in value:
            sanitized = clean(child, visible, counts)
            if sanitized is not common.DROP:
                result.append(sanitized)
        return result
    return value


def read_indexes(connection, identities):
    result = {table: [] for table in common.KEYS}
    for identity in identities:
        for table, rows in common.read_tables(connection, identity).items():
            result[table].extend(rows)
    return result


def validate(segment, tables, identity):
    records, start, offsets = segment["records"], segment["start"], segment["offsets"]
    cursor = tables["thread_history_projection_state"]
    require(len(cursor) == 1 and (cursor[0]["next_rollout_ordinal"], cursor[0]["next_rollout_byte_offset"]) ==
            (start + len(records), len(segment["data"])), "Segment index is not at EOF.")
    for table, rows in tables.items():
        for row in rows:
            require(row["thread_id"] == identity, "Wrong segment index identity.")
            for column in common.ORDINAL_COLUMNS & row.keys():
                value = row[column]
                require(value is None or (column == "updated_at_ordinal" and value == 0) or start <= value < start + len(records), "Out-of-segment index ordinal.")
            for column in common.OFFSET_COLUMNS & row.keys():
                require(row[column] is None or row[column] in offsets, "Index byte offset is not a line boundary.")
    for row in tables["thread_turns"]:
        at = row["rollout_ordinal"] - start
        payload = records[at].get("payload", {})
        require(payload.get("type") == "task_started" and payload.get("turn_id") == row["turn_id"], "Wrong turn source record.")
        require(row["rollout_byte_offset"] == offsets[at], "Wrong turn start offset.")
        if row["rollout_end_ordinal"] is not None:
            require(row["rollout_end_byte_offset"] == offsets[row["rollout_end_ordinal"] - start + 1], "Wrong turn end offset.")
    for row in tables["thread_items"]:
        payload = records[row["rollout_ordinal"] - start].get("payload", {})
        require(payload.get("item", {}).get("id") == row["item_id"] and payload.get("turn_id") == row["turn_id"], "Wrong item source record.")


def plan(home, thread_id, catalog_entries=None):
    metadata = common.directory_metadata(home, thread_id)
    latest = common.normalized_path(metadata["state_thread"]["rollout_path"])
    require(any(latest.is_relative_to(home / folder) for folder in ("sessions", "archived_sessions")), "Rollout is outside this Codex home's session directories.")
    entries = catalog_entries if catalog_entries is not None else catalog(home)
    chain, seen, path = [], set(), latest
    while True:
        identity = physical_id(path)
        require(identity not in seen, "Cyclic history chain.")
        seen.add(identity)
        segment = {"path": path, "physical_id": identity, **parse(path.read_bytes(), thread_id)}
        chain.append(segment)
        base = segment["records"][0]["payload"].get("history_base")
        if not base:
            break
        candidates = entries.get(base["thread_id"], [])
        require(len(candidates) == 1, "Ambiguous or missing base segment.")
        path = candidates[0][0]
    chain.reverse()
    require(chain[0]["start"] == 0, "The first segment must begin at ordinal zero.")
    check_references(entries, {s["path"] for s in chain}, seen)
    last_plaintext_checkpoint = -1
    for index, segment in enumerate(chain):
        limit = len(segment["data"])
        if index + 1 < len(chain):
            base = chain[index + 1]["records"][0]["payload"]["history_base"]
            limit = base["end_byte_offset"]
            require(limit in segment["offsets"] and base["end_ordinal_exclusive"] == segment["start"] + segment["offsets"].index(limit), "Base reference is not synchronized.")
        for row, offset in zip(segment["records"], segment["offsets"]):
            payload = row.get("payload", {})
            if offset < limit and row.get("type") == "compacted" and payload.get("message") and not common.encrypted_count(payload.get("replacement_history")):
                last_plaintext_checkpoint = max(last_plaintext_checkpoint, row["ordinal"])
    visible = {r["payload"]["item"]["id"]: r["payload"]["item"] for s in chain for r in s["records"]
               if r.get("payload", {}).get("type") == "item_completed"}
    connection = common.connect_readonly(home / "thread_history_1.sqlite")
    try:
        connection.execute("BEGIN")
        before = read_indexes(connection, [s["physical_id"] for s in chain])
    finally:
        connection.close()
    after, transformed, counts = {table: [] for table in common.KEYS}, {}, Counter()
    for segment in chain:
        identity = segment["physical_id"]
        old_tables = {table: [row for row in rows if row["thread_id"] == identity] for table, rows in before.items()}
        validate(segment, old_tables, identity)
        base = segment["records"][0]["payload"].get("history_base")
        new_base = None
        if base:
            parent = transformed[base["thread_id"]]
            new_base = {**base, "end_ordinal_exclusive": parent["boundaries"][base["end_ordinal_exclusive"]],
                        "end_byte_offset": parent["byte_map"][base["end_byte_offset"]]}
        new_start = new_base["end_ordinal_exclusive"] if new_base else 0
        parts, ordinal_map, boundaries, byte_map, size = [], {}, {}, {}, 0
        for local, (raw, record) in enumerate(zip(segment["lines"], segment["records"])):
            old_ordinal = record["ordinal"]
            boundaries[old_ordinal], byte_map[segment["offsets"][local]] = new_start + len(parts), size
            payload = record.get("payload", {})
            if record.get("type") == "compacted" and any(item.get("type") == "compaction" and item.get("encrypted_content") for item in payload.get("replacement_history", [])):
                require(not payload.get("message") and old_ordinal < last_plaintext_checkpoint,
                        "An active opaque compaction checkpoint cannot be discarded.")
                counts["removed_superseded_opaque_checkpoints"] += 1
                continue
            cleaned = clean(record, visible, counts)
            if cleaned is common.DROP:
                counts["removed_rollout_reasoning_records"] += 1
                continue
            if local == 0 and new_base:
                cleaned["payload"]["history_base"] = new_base
            new_ordinal = new_start + len(parts)
            ordinal_map[old_ordinal] = new_ordinal
            cleaned["ordinal"] = new_ordinal
            encoded = raw if cleaned == record else (json.dumps(cleaned, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
            parts.append(encoded)
            size += len(encoded)
        boundaries[segment["start"] + len(segment["records"])] = new_start + len(parts)
        byte_map[len(segment["data"])] = size
        # Older schemas use zero as the uninitialized updated_at_ordinal sentinel.
        if segment["start"]:
            ordinal_map[0] = 0
        new_tables = common.remap_tables(old_tables, ordinal_map, boundaries, byte_map)
        candidate = parse(b"".join(parts), thread_id)
        require(common.encrypted_count(candidate["records"]) == 0, "Encrypted content remains in a segment.")
        validate(candidate, new_tables, identity)
        def protected(records):
            return [{k: v for k, v in row.items() if k != "ordinal"} for row in records
                    if row.get("type") not in {"session_meta", "compacted"}
                    and not (row.get("type") == "response_item" and row.get("payload", {}).get("type") in {"reasoning", "agent_message"})]
        require(protected(segment["records"]) == protected(candidate["records"]), "Visible events, messages or tool data changed.")
        transformed[identity] = {"candidate": candidate, "boundaries": boundaries, "byte_map": byte_map}
        for table, rows in new_tables.items():
            after[table].extend(rows)
    summary = {"format": "split-v1", "session_id": thread_id, "codex_home": str(home), "source": str(latest),
               "segment_count": len(chain), "physical_ids": [s["physical_id"] for s in chain],
               "encrypted_fields_before": sum(common.encrypted_count(s["records"]) for s in chain), "encrypted_fields_after": 0,
               "transformations": dict(counts), "index_row_counts": {t: len(r) for t, r in before.items()},
               "files": [{"path": str(s["path"]), "physical_id": s["physical_id"], "before_sha256": common.digest(s["data"]),
                          "after_sha256": common.digest(transformed[s["physical_id"]]["candidate"]["data"]),
                          "before_records": len(s["records"]), "after_records": len(transformed[s["physical_id"]]["candidate"]["records"])} for s in chain]}
    return summary, chain, transformed, before, after, metadata


def prepare(home, thread_id):
    summary, chain, transformed, before, after, metadata = plan(home, thread_id)
    require(summary["encrypted_fields_before"] > 0, "This history is already clean.")
    backup = home / "backups_state" / ("encrypted-split-repair-" + thread_id + "-" + datetime.now().strftime("%Y%m%dT%H%M%S%f"))
    (backup / "original").mkdir(parents=True)
    (backup / "repaired").mkdir()
    for segment in chain:
        common.write_new(backup / "original" / segment["path"].name, segment["data"])
        common.write_new(backup / "repaired" / segment["path"].name, transformed[segment["physical_id"]]["candidate"]["data"])
    for filename, content in (("history-index.before.json", before), ("history-index.after.json", after), ("directory-metadata.before.json", metadata)):
        common.save_json(backup / filename, content)
    manifest = {**summary, "backup": str(backup), "state": "prepared"}
    common.save_json(backup / "manifest.json", manifest)
    return backup, manifest


def apply_backup(backup, restore=False):
    manifest = common.read_json(backup / "manifest.json")
    home, identity = common.normalized_path(manifest["codex_home"]), manifest["session_id"]
    require(backup.is_relative_to(home / "backups_state"), "Unexpected backup location.")
    expected_side, wanted_side = ("after", "before") if restore else ("before", "after")
    expected = common.read_json(backup / ("history-index." + expected_side + ".json"))
    wanted = common.read_json(backup / ("history-index." + wanted_side + ".json"))
    metadata = common.read_json(backup / "directory-metadata.before.json")
    paths = [common.normalized_path(info["path"]) for info in manifest["files"]]
    require(all(any(path.is_relative_to(home / folder) for folder in ("sessions", "archived_sessions")) for path in paths), "Unexpected rollout path.")
    target_bytes = [(backup / ("original" if restore else "repaired") / path.name).read_bytes() for path in paths]
    for data, info in zip(target_bytes, manifest["files"]):
        require(common.digest(data) == info[wanted_side + "_sha256"], "Candidate/backup checksum mismatch.")
    stages, replaced = [], []
    with ExitStack() as stack:
        for physical in sorted(manifest["physical_ids"]):
            stack.enter_context(common.thread_guard(home, physical))
        check_references(catalog(home), set(paths), set(manifest["physical_ids"]))
        originals = [path.read_bytes() for path in paths]
        for data, info in zip(originals, manifest["files"]):
            require(common.digest(data) == info[expected_side + "_sha256"], "History changed; refusing to overwrite new messages.")
        require(common.directory_metadata(home, identity) == metadata, "Directory metadata changed.")
        database = home / "thread_history_1.sqlite"
        connection = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN IMMEDIATE")
            require(common.canonical_tables(read_indexes(connection, manifest["physical_ids"])) == common.canonical_tables(expected), "History index changed.")
            common.update_positions(connection, expected, wanted)
            for path, data in zip(paths, target_bytes):
                stage = path.with_name(path.name + ".split-repair-" + uuid.uuid4().hex + ".tmp")
                common.write_new(stage, data)
                stages.append(stage)
            for path, info in zip(paths, manifest["files"]):
                require(common.digest(path.read_bytes()) == info[expected_side + "_sha256"], "History changed during preparation.")
            for index, (stage, path) in enumerate(zip(stages, paths)):
                os.replace(stage, path)
                replaced.append(index)
            require(common.canonical_tables(read_indexes(connection, manifest["physical_ids"])) == common.canonical_tables(wanted), "Index update mismatch.")
            for path, info in zip(paths, manifest["files"]):
                require(common.digest(path.read_bytes()) == info[wanted_side + "_sha256"], "Written file checksum mismatch.")
            connection.commit()
        except BaseException:
            connection.rollback()
            require(common.canonical_tables(read_indexes(connection, manifest["physical_ids"])) == common.canonical_tables(expected), "Uncertain database commit; inspect the backup.")
            for index in reversed(replaced):
                require(paths[index].read_bytes() == target_bytes[index], "External file change prevents rollback.")
                common.write_new(stages[index], originals[index])
                os.replace(stages[index], paths[index])
            raise
        finally:
            connection.close()
            for stage in stages:
                if stage.exists():
                    stage.unlink()
        require(common.directory_metadata(home, identity) == metadata, "Directory metadata changed during repair.")
        manifest["state"] = "restored" if restore else "applied"
        manifest[manifest["state"] + "_at"] = datetime.now().astimezone().isoformat()
        common.save_json(backup / "manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("thread_id", nargs="?")
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--apply-backup", type=Path)
    parser.add_argument("--restore", type=Path)
    args = parser.parse_args()
    if args.apply_backup or args.restore:
        result = apply_backup((args.apply_backup or args.restore).resolve(), restore=bool(args.restore))
    elif args.prepare or args.apply:
        folder, result = prepare(Path.home() / ".codex", args.thread_id)
        if args.apply:
            result = apply_backup(folder)
    else:
        result = {"state": "dry_run", **plan(Path.home() / ".codex", args.thread_id)[0]}
    print(json.dumps(result, ensure_ascii=False, indent=2))
