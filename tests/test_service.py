import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
import zipfile

import repair_encrypted_session as engine
import repair_split_encrypted_session as split
from repair_service import RepairService
from fixtures import ROOT_ID, CHILD_ID, GRANDCHILD_ID, SPLIT_ID, OTHER_ID, demo_home


def wait(service, job):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        result = service.jobs.get(job["id"])
        if result["state"] in {"succeeded", "failed"}:
            return result
        time.sleep(.01)
    raise AssertionError("Test job did not finish")


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="csr-")
        self.home = demo_home(Path(self.temp.name) / "home")
        self.service = RepairService(self.home)
        self.addCleanup(self.clean_up)

    def clean_up(self):
        self.service.jobs.executor.shutdown(wait=True)
        assert Path(self.temp.name).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve())
        self.temp.cleanup()

    def inspect(self, identity=ROOT_ID):
        result = wait(self.service, self.service.inspect(identity, True))
        self.assertEqual(result["state"], "succeeded", result)
        return result["result"]

    def clean(self, selected=(ROOT_ID, CHILD_ID), identity=ROOT_ID):
        inspected = self.inspect(identity)
        result = wait(self.service, self.service.clean(inspected["inspection_id"], list(selected)))
        self.assertEqual(result["state"], "succeeded", result)
        return result["result"]["backup"]

    def all_files(self):
        return {str(path): path.read_bytes() for path in (self.home / "sessions").glob("*.jsonl")}

    def indexes(self):
        con = engine.connect_readonly(self.home / "thread_history_1.sqlite")
        try:
            return {table: [dict(row) for row in con.execute("SELECT * FROM " + table + " ORDER BY thread_id")] for table in engine.KEYS}
        finally:
            con.close()

    def test_resume_command_finds_child_and_family_is_recursive(self):
        result = self.service.catalog.search("codex resume " + CHILD_ID, "main")
        self.assertEqual([row["id"] for row in result["items"]], [CHILD_ID])
        family = self.service.catalog.family(ROOT_ID)
        self.assertEqual([(n["id"], n["depth"]) for n in family["nodes"]], [(ROOT_ID, 0), (CHILD_ID, 1), (GRANDCHILD_ID, 2)])

    def test_inspection_counts_nested_cipher_without_exposing_it(self):
        result = self.inspect()
        by_id = {row["id"]: row for row in result["nodes"]}
        self.assertEqual(by_id[ROOT_ID]["encrypted_blocks"], 3)
        self.assertEqual(by_id[CHILD_ID]["encrypted_blocks"], 3)
        self.assertEqual(by_id[GRANDCHILD_ID]["encrypted_blocks"], 0)
        self.assertTrue(by_id[ROOT_ID]["error_evidence"]["latest_failed"])
        self.assertNotIn("TEST-SECRET-CIPHER", json.dumps(result))

    def test_group_clean_and_restore_preserve_visible_items_and_unselected_files(self):
        original_files, before_indexes = self.all_files(), self.indexes()
        backup = self.clean()
        bundle = self.service._load_bundle(backup["id"])
        selected_paths = {row["path"] for row in bundle["files"]}
        for path, data in self.all_files().items():
            if path in selected_paths:
                self.assertEqual(engine.encrypted_count([json.loads(raw) for raw in data.splitlines()]), 0)
            else:
                self.assertEqual(data, original_files[path])
        before_items = {r["item_id"]: r["item_json"] for r in before_indexes["thread_items"]}
        after_items = {r["item_id"]: r["item_json"] for r in self.indexes()["thread_items"]}
        self.assertEqual(before_items, after_items)
        restored = wait(self.service, self.service.restore(backup["id"]))
        self.assertEqual(restored["state"], "succeeded", restored)
        self.assertEqual(self.all_files(), original_files)
        self.assertEqual(engine.canonical_tables(self.indexes()), engine.canonical_tables(before_indexes))

    def test_split_chain_clean_and_rollback(self):
        before = self.all_files()
        backup = self.clean((SPLIT_ID,), SPLIT_ID)
        self.assertEqual(backup["segments"], 3)
        bundle = self.service._load_bundle(backup["id"])
        self.assertTrue(self.service._verify_wanted(bundle, "after")["history_references"])
        result = wait(self.service, self.service.restore(backup["id"]))
        self.assertEqual(result["state"], "succeeded", result)
        self.assertEqual(self.all_files(), before)

    def test_new_message_after_inspection_prevents_cleaning(self):
        inspected = self.inspect()
        path = self.service.catalog.source(ROOT_ID)
        original = path.read_bytes()
        row = {"ordinal": len(original.splitlines()), "type": "event_msg", "payload": {"type": "task_started", "turn_id": "new-turn"}}
        changed = original + (json.dumps(row) + "\n").encode()
        path.write_bytes(changed)
        result = wait(self.service, self.service.clean(inspected["inspection_id"], [ROOT_ID]))
        self.assertEqual(result["state"], "failed")
        self.assertEqual(path.read_bytes(), changed)

    def test_new_message_blocks_rollback_and_backup_remains_exportable(self):
        backup = self.clean((ROOT_ID,))
        path = self.service.catalog.source(ROOT_ID)
        changed = path.read_bytes() + b'{"ordinal":999,"type":"event_msg","payload":{"text":"NEW MESSAGE"}}\n'
        path.write_bytes(changed)
        result = wait(self.service, self.service.restore(backup["id"]))
        self.assertEqual(result["state"], "failed")
        self.assertEqual(path.read_bytes(), changed)
        output = Path(self.temp.name) / "backup.zip"
        self.service.export_backup(backup["id"], output)
        with zipfile.ZipFile(output) as archive:
            self.assertIn("manifest.json", archive.namelist())
            self.assertTrue(any(name.startswith("before/") for name in archive.namelist()))

    def test_busy_child_is_detected_without_blocking_read_only_inspection(self):
        lock = self.home / "thread-writer-locks" / (CHILD_ID + ".lock")
        lock.parent.mkdir()
        lock.write_text("held")
        result = self.inspect()
        nodes = {node["id"]: node for node in result["nodes"]}
        self.assertFalse(nodes[CHILD_ID]["ready"])
        self.assertTrue(nodes[ROOT_ID]["ready"])
        self.assertIn("占用", nodes[CHILD_ID]["reason"])

    def test_partial_group_swap_failure_rolls_back_both_files_and_indexes(self):
        original_files, original_indexes = self.all_files(), self.indexes()
        inspected = self.inspect()
        replace, calls = self.service._replace_from_blob, 0
        def fail_once(bundle, item, side):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("simulated file failure")
            return replace(bundle, item, side)
        with mock.patch.object(self.service, "_replace_from_blob", side_effect=fail_once):
            result = wait(self.service, self.service.clean(inspected["inspection_id"], [ROOT_ID, CHILD_ID]))
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.all_files(), original_files)
        self.assertEqual(engine.canonical_tables(self.indexes()), engine.canonical_tables(original_indexes))

    def test_recovery_handles_files_after_commit_with_database_before(self):
        originals = self.all_files()
        backup = self.clean()
        bundle = self.service._load_bundle(backup["id"])
        con = engine.connect_readonly(self.home / "thread_history_1.sqlite")
        con.close()
        import sqlite3
        con = sqlite3.connect(self.home / "thread_history_1.sqlite")
        engine.update_positions(con, bundle["after"], bundle["before"])
        con.commit()
        con.close()
        first = bundle["files"][0]
        Path(first["path"]).write_bytes(originals[first["path"]])
        self.service._save_state(bundle, "applying")
        result = wait(self.service, self.service.restore(backup["id"], recover=True))
        self.assertEqual(result["state"], "succeeded", result)
        self.assertEqual(self.all_files(), originals)
        self.assertEqual(self.service._load_bundle(backup["id"])["manifest"]["state"], "recovered")

    def test_failed_restore_keeps_the_applied_backup_available_for_retry(self):
        originals = self.all_files()
        backup = self.clean()
        repaired = self.all_files()
        replace, calls = self.service._replace_from_blob, 0
        def fail_once(bundle, item, side):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("simulated restore failure")
            return replace(bundle, item, side)
        with mock.patch.object(self.service, "_replace_from_blob", side_effect=fail_once):
            result = wait(self.service, self.service.restore(backup["id"]))
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.all_files(), repaired)
        self.assertEqual(self.service._load_bundle(backup["id"])["manifest"]["state"], "applied")
        retried = wait(self.service, self.service.restore(backup["id"]))
        self.assertEqual(retried["state"], "succeeded", retried)
        self.assertEqual(self.all_files(), originals)

    def test_backup_path_tampering_cannot_write_outside_the_home(self):
        backup = self.clean((ROOT_ID,))
        bundle = self.service._load_bundle(backup["id"])
        manifest = bundle["manifest"]
        target = Path(self.temp.name) / "unrelated.txt"
        target.write_text("keep")
        manifest["files"][0]["path"] = str(target)
        engine.save_json(bundle["folder"] / "manifest.json", manifest)
        result = wait(self.service, self.service.restore(backup["id"]))
        self.assertEqual(result["state"], "failed")
        self.assertEqual(target.read_text(), "keep")


if __name__ == "__main__":
    unittest.main(verbosity=2)
