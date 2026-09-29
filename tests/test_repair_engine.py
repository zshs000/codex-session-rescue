"""Regression checks for destructive-history and SQLite-position failure modes."""

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import repair_encrypted_session as repair


THREAD = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"


def encode(records):
    return b"".join((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8") for record in records)


class RepairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        (self.home / "sessions").mkdir()
        self.source = self.home / "sessions" / f"rollout-{THREAD}.jsonl"
        payloads = [
            ("session_meta", {"id": THREAD}),
            ("event_msg", {"type": "task_started", "turn_id": "turn-1"}),
            ("response_item", {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "中文 user question"}]}),
            ("event_msg", {"type": "item_completed", "turn_id": "turn-1", "item": {"id": "user-1", "type": "UserMessage"}}),
            ("response_item", {"type": "reasoning", "id": "rs-1", "summary": [], "encrypted_content": "opaque-test-value"}),
            ("response_item", {"type": "agent_message", "id": "agent-1", "content": [
                {"type": "input_text", "text": "Keep this agent message 中文"},
                {"type": "encrypted_content", "encrypted_content": "opaque-agent-value"}]}),
            ("compacted", {"message": "Keep the summary", "replacement_history": [{"type": "message", "role": "user", "content": []}],
                           "guardian_history": [
                               {"type": "reasoning", "summary": [], "encrypted_content": "opaque-guardian-value"},
                               {"type": "agent_message", "content": [
                                   {"type": "input_text", "text": "Keep nested agent text"},
                                   {"type": "encrypted_content", "encrypted_content": "nested-opaque-value"}]}]}),
            ("response_item", {"type": "custom_tool_call", "input": 'Do not alter this string: {"encrypted_content":"example"}'}),
            ("event_msg", {"type": "item_completed", "turn_id": "turn-1", "item": {"id": "answer-1", "type": "AgentMessage"}}),
            ("response_item", {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Keep the answer"}]}),
            ("event_msg", {"type": "token_count"}),
            ("event_msg", {"type": "task_complete", "turn_id": "turn-1"}),
        ]
        self.records = [{"timestamp": "2026-09-18T00:00:00Z", "ordinal": i, "type": kind, "payload": payload}
                        for i, (kind, payload) in enumerate(payloads)]
        self.original = encode(self.records)
        self.source.write_bytes(self.original)
        rollout = repair.Rollout.parse(self.original, THREAD)
        state = sqlite3.connect(self.home / "state_5.sqlite")
        state.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT, history_mode TEXT, title TEXT)")
        state.execute("INSERT INTO threads VALUES (?,?,?,?)", (THREAD, str(self.source), "paginated", "Keep title"))
        state.commit()
        state.close()
        (self.home / "session_index.jsonl").write_text(json.dumps({"id": THREAD, "thread_name": "Keep title"}) + "\n", encoding="utf-8")
        database = sqlite3.connect(self.home / "thread_history_1.sqlite")
        database.executescript("""
            CREATE TABLE thread_history_projection_state (thread_id TEXT PRIMARY KEY, next_rollout_byte_offset INTEGER, next_rollout_ordinal INTEGER);
            CREATE TABLE thread_turns (thread_id TEXT, turn_id TEXT, rollout_ordinal INTEGER, rollout_byte_offset INTEGER, rollout_end_ordinal INTEGER, rollout_end_byte_offset INTEGER, status TEXT, PRIMARY KEY (thread_id, turn_id));
            CREATE TABLE thread_items (thread_id TEXT, turn_id TEXT, item_id TEXT, rollout_ordinal INTEGER, updated_at_ordinal INTEGER, item_json TEXT, PRIMARY KEY (thread_id, turn_id, item_id));
            CREATE TABLE thread_realtime_items (thread_id TEXT, item_id TEXT, rollout_ordinal INTEGER, item_json TEXT, PRIMARY KEY (thread_id, item_id));
        """)
        database.execute("INSERT INTO thread_history_projection_state VALUES (?,?,?)", (THREAD, len(self.original), len(self.records)))
        database.execute("INSERT INTO thread_history_projection_state VALUES (?,?,?)", (OTHER, 100, 5))
        database.execute("INSERT INTO thread_turns VALUES (?,?,?,?,?,?,?)", (THREAD, "turn-1", 1, rollout.offsets[1], 11, len(self.original), "completed"))
        database.execute("INSERT INTO thread_items VALUES (?,?,?,?,?,?)", (THREAD, "turn-1", "user-1", 3, 3, '{"text":"original question"}'))
        database.execute("INSERT INTO thread_items VALUES (?,?,?,?,?,?)", (THREAD, "turn-1", "answer-1", 8, 8, '{"text":"original answer"}'))
        database.execute("INSERT INTO thread_items VALUES (?,?,?,?,?,?)", (OTHER, "other-turn", "other-item", 3, 3, '{"text":"another thread"}'))
        database.commit()
        database.close()

    def tables(self, thread_id=THREAD):
        connection = repair.connect_readonly(self.home / "thread_history_1.sqlite")
        try:
            return repair.read_tables(connection, thread_id)
        finally:
            connection.close()

    def test_nested_cleanup_utf8_offsets_and_visible_content(self):
        summary, original, candidate, before, after, _ = repair.make_plan(self.home, THREAD)
        self.assertEqual(summary["encrypted_fields_before"], 4)
        self.assertEqual(len(candidate.records), 11)
        self.assertEqual(repair.encrypted_count(candidate.records), 0)
        self.assertEqual([r["ordinal"] for r in candidate.records], list(range(11)))
        self.assertEqual(candidate.records[4]["payload"]["content"][0]["text"], "Keep this agent message 中文")
        self.assertEqual(candidate.records[5]["payload"]["guardian_history"][0]["content"][0]["text"], "Keep nested agent text")
        self.assertEqual(candidate.records[6]["payload"], self.records[7]["payload"])
        answer = next(row for row in after["thread_items"] if row["item_id"] == "answer-1")
        self.assertEqual(answer["rollout_ordinal"], 7)
        self.assertEqual(answer["updated_at_ordinal"], 7)
        self.assertEqual(after["thread_turns"][0]["rollout_end_ordinal"], 10)
        self.assertEqual(after["thread_turns"][0]["rollout_end_byte_offset"], len(candidate.data))
        self.assertEqual(after["thread_history_projection_state"][0]["next_rollout_byte_offset"], len(candidate.data))
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.tables(), before)

    def test_apply_restore_and_other_thread_unchanged(self):
        before, other = self.tables(), self.tables(OTHER)
        metadata = repair.directory_metadata(self.home, THREAD)
        backup, _ = repair.prepare(self.home, THREAD)
        applied = repair.apply_backup(backup)
        self.assertEqual(applied["state"], "applied")
        self.assertEqual(repair.encrypted_count(repair.Rollout.parse(self.source.read_bytes(), THREAD).records), 0)
        self.assertEqual(self.tables(OTHER), other)
        self.assertEqual(repair.directory_metadata(self.home, THREAD), metadata)
        self.assertFalse((self.home / "thread-writer-locks" / (THREAD + ".lock")).exists())
        restored = repair.apply_backup(backup, restore=True)
        self.assertEqual(restored["state"], "restored")
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.tables(), before)
        self.assertEqual(self.tables(OTHER), other)

    def test_appended_message_blocks_apply(self):
        backup, _ = repair.prepare(self.home, THREAD)
        changed = self.original + b'{"ordinal":12,"type":"event_msg","payload":{"type":"task_started"}}\n'
        self.source.write_bytes(changed)
        before = self.tables()
        with self.assertRaisesRegex(repair.RepairError, "refusing to overwrite"):
            repair.apply_backup(backup)
        self.assertEqual(self.source.read_bytes(), changed)
        self.assertEqual(self.tables(), before)

    def test_appended_message_blocks_restore(self):
        backup, _ = repair.prepare(self.home, THREAD)
        repair.apply_backup(backup)
        changed = self.source.read_bytes() + b'{"ordinal":11,"type":"event_msg","payload":{}}\n'
        self.source.write_bytes(changed)
        before = self.tables()
        with self.assertRaisesRegex(repair.RepairError, "refusing to overwrite"):
            repair.apply_backup(backup, restore=True)
        self.assertEqual(self.source.read_bytes(), changed)
        self.assertEqual(self.tables(), before)

    def test_changed_index_blocks_apply(self):
        backup, _ = repair.prepare(self.home, THREAD)
        connection = sqlite3.connect(self.home / "thread_history_1.sqlite")
        connection.execute("UPDATE thread_items SET item_json=? WHERE thread_id=? AND item_id=?", ('{"text":"newer answer"}', THREAD, "answer-1"))
        connection.commit()
        connection.close()
        changed = self.tables()
        with self.assertRaisesRegex(repair.RepairError, "index changed"):
            repair.apply_backup(backup)
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.tables(), changed)

    def test_writer_lock_blocks_apply(self):
        backup, _ = repair.prepare(self.home, THREAD)
        lock = self.home / "thread-writer-locks" / (THREAD + ".lock")
        lock.parent.mkdir()
        lock.write_text("held", encoding="utf-8")
        with self.assertRaisesRegex(repair.RepairError, "writer lock"):
            repair.apply_backup(backup)
        self.assertEqual(lock.read_text(encoding="utf-8"), "held")
        self.assertEqual(self.source.read_bytes(), self.original)

    def test_duplicate_ordinal_is_rejected(self):
        self.records[4]["ordinal"] = 3
        self.source.write_bytes(encode(self.records))
        with self.assertRaisesRegex(repair.RepairError, "Non-contiguous"):
            repair.make_plan(self.home, THREAD)

    def test_shared_history_is_rejected(self):
        fork = self.home / "sessions" / f"rollout-{OTHER}.jsonl"
        fork.write_text(json.dumps({"type": "session_meta", "payload": {"id": OTHER, "history_base": {"thread_id": THREAD}}}) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(repair.RepairError, "shared history"):
            repair.make_plan(self.home, THREAD)

    def test_unknown_or_readable_encryption_is_rejected(self):
        with self.assertRaisesRegex(repair.RepairError, "Unsupported"):
            repair.strip_opaque({"type": "unknown", "encrypted_content": "test"}, repair.Counter())
        with self.assertRaisesRegex(repair.RepairError, "readable content"):
            repair.strip_opaque({"type": "reasoning", "summary": [{"text": "Keep this"}], "encrypted_content": "test"}, repair.Counter())

    def test_removed_record_index_reference_is_rejected(self):
        connection = sqlite3.connect(self.home / "thread_history_1.sqlite")
        connection.execute("UPDATE thread_items SET updated_at_ordinal=4 WHERE thread_id=? AND item_id=?", (THREAD, "user-1"))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(repair.RepairError, "removed position"):
            repair.make_plan(self.home, THREAD)

    def test_file_replace_failure_rolls_back_sqlite(self):
        backup, _ = repair.prepare(self.home, THREAD)
        before = self.tables()
        replace = repair.os.replace
        def fail_target(source, target):
            if target == self.source:
                raise OSError("simulated replacement failure")
            return replace(source, target)
        with mock.patch.object(repair.os, "replace", side_effect=fail_target):
            with self.assertRaisesRegex(OSError, "simulated"):
                repair.apply_backup(backup)
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.tables(), before)
        self.assertEqual(list(self.source.parent.glob("*.tmp")), [])

    def test_commit_failure_restores_file_and_index(self):
        backup, _ = repair.prepare(self.home, THREAD)
        before = self.tables()
        connect = sqlite3.connect
        class FailingCommit(sqlite3.Connection):
            def commit(self):
                raise sqlite3.OperationalError("simulated commit failure")
        def connect_with_failure(database, *args, **kwargs):
            if str(database).endswith("?mode=rw"):
                kwargs["factory"] = FailingCommit
            return connect(database, *args, **kwargs)
        with mock.patch.object(repair.sqlite3, "connect", side_effect=connect_with_failure):
            with self.assertRaisesRegex(sqlite3.OperationalError, "simulated"):
                repair.apply_backup(backup)
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.tables(), before)
        self.assertEqual(list(self.source.parent.glob("*.tmp")), [])

    def test_unique_ordinal_index_allows_apply_and_restore(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE thread_turns (thread_id TEXT, turn_id TEXT, rollout_ordinal INTEGER, PRIMARY KEY(thread_id,turn_id), UNIQUE(thread_id,rollout_ordinal))")
        before = {table: [] for table in repair.KEYS}
        after = {table: [] for table in repair.KEYS}
        for ordinal in (10, 11, 12):
            row = {"thread_id": THREAD, "turn_id": str(ordinal), "rollout_ordinal": ordinal}
            before["thread_turns"].append(row)
            after["thread_turns"].append({**row, "rollout_ordinal": ordinal + 1})
            connection.execute("INSERT INTO thread_turns VALUES (?,?,?)", (THREAD, str(ordinal), ordinal))
        repair.update_positions(connection, before, after)
        self.assertEqual([row[0] for row in connection.execute("SELECT rollout_ordinal FROM thread_turns ORDER BY rollout_ordinal")], [11, 12, 13])
        repair.update_positions(connection, after, before)
        self.assertEqual([row[0] for row in connection.execute("SELECT rollout_ordinal FROM thread_turns ORDER BY rollout_ordinal")], [10, 11, 12])


if __name__ == "__main__":
    unittest.main(verbosity=2)
