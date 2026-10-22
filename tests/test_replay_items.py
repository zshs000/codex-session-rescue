from collections import Counter
import json
import unittest

import repair_encrypted_session as single
import repair_split_encrypted_session as split
from replay_items import normalize_history
from session_gateway import filter_request


CALL = {"type": "function_call", "id": "fc_provider_bound", "call_id": "call_stable",
        "name": "save", "arguments": '{"id":"business-id","encrypted_content":"business value"}',
        "status": "completed"}
OUTPUT = {"type": "function_call_output", "id": "fco_provider_bound", "call_id": "call_stable",
          "output": '{"id":"business-output","encrypted_content":"business value"}'}
REASONING = {"type": "reasoning", "id": "rs_provider_bound", "summary": [], "encrypted_content": "CIPHER"}


class ReplayItemTests(unittest.TestCase):
    def test_gateway_detaches_tool_server_ids_even_when_no_cipher_remains(self):
        original = {"input": [CALL, OUTPUT]}
        candidate, removed = filter_request(original)
        self.assertEqual(removed, 0)
        self.assertEqual(candidate["input"], [{k: v for k, v in item.items() if k != "id"} for item in (CALL, OUTPUT)])
        self.assertEqual(CALL["id"], "fc_provider_bound")
        self.assertEqual(candidate["input"][0]["call_id"], candidate["input"][1]["call_id"])

    def test_gateway_removes_cipher_and_preserves_function_arguments_results(self):
        candidate, removed = filter_request({"input": [REASONING, CALL, OUTPUT]})
        self.assertEqual(removed, 1)
        self.assertEqual(len(candidate["input"]), 2)
        self.assertEqual(candidate["input"][0]["arguments"], CALL["arguments"])
        self.assertEqual(candidate["input"][1]["output"], OUTPUT["output"])
        self.assertNotIn("id", candidate["input"][0])

    def test_single_file_cleanup_detaches_calls_but_retains_visible_event_ids(self):
        identity = "11111111-1111-4111-8111-111111111111"
        payloads = [("session_meta", {"id": identity}), ("response_item", REASONING),
                    ("response_item", CALL), ("response_item", OUTPUT),
                    ("event_msg", {"type": "item_completed", "item": {"id": CALL["id"], "type": "toolCall"}})]
        records = [{"ordinal": i, "type": kind, "payload": value} for i, (kind, value) in enumerate(payloads)]
        raw = b"".join((json.dumps(r) + "\n").encode() for r in records)
        candidate, _, _, _, counts = single.build_candidate(single.Rollout.parse(raw, identity), identity)
        calls = [r["payload"] for r in candidate.records if r["type"] == "response_item"]
        self.assertEqual(calls, [{k: v for k, v in item.items() if k != "id"} for item in (CALL, OUTPUT)])
        self.assertEqual(candidate.records[-1]["payload"]["item"]["id"], CALL["id"])
        self.assertEqual(counts["detached_server_item_ids"], 2)

    def test_provider_message_id_is_detached_but_local_message_and_ui_ids_are_kept(self):
        provider = {"type": "message", "id": "msg_0d0c568e89685e2c006ad9dfc058948197857f12380356f4da",
                    "role": "assistant", "status": "completed", "phase": "commentary",
                    "content": [{"type": "output_text", "text": "Preserve this answer", "annotations": []}]}
        local = {"type": "message", "id": "msg_01a1630c-c110-7d83-9210-2b506409ea37",
                 "role": "user", "content": [{"type": "input_text", "text": "Keep my question"}]}
        candidate, removed = filter_request({"input": [provider, local]})
        self.assertEqual(removed, 0)
        self.assertNotIn("id", candidate["input"][0])
        self.assertEqual(candidate["input"][0], {k:v for k,v in provider.items() if k != "id"})
        self.assertEqual(candidate["input"][1], local)
        record = {"type": "response_item", "payload": provider}
        normalized, count = normalize_history(record)
        self.assertEqual(count, 1)
        self.assertNotIn("id", normalized["payload"])
        event = {"type": "event_msg", "payload": {"type": "item_completed", "item": provider}}
        self.assertEqual(normalize_history(event), (event, 0))

    def test_nested_compaction_history_is_detached_and_arbitrary_tool_json_is_untouched(self):
        record = {"type": "compacted", "payload": {"replacement_history": [CALL, OUTPUT],
                  "guardian_history": [{"type": "response_item", "payload": CALL}]}}
        candidate, count = normalize_history(record)
        self.assertEqual(count, 3)
        self.assertNotIn("id", candidate["payload"]["replacement_history"][0])
        self.assertEqual(candidate["payload"]["replacement_history"][0]["arguments"], CALL["arguments"])
        # Ordinary business payloads and UI items must not be rewritten.
        business = {"type": "event_msg", "payload": {"type": "function_call", "id": "business"}}
        self.assertEqual(normalize_history(business), (business, 0))

    def test_split_cleanup_uses_same_replay_rules(self):
        record = {"ordinal": 30, "type": "response_item", "payload": CALL}
        candidate, count = normalize_history(split.clean(record, {}, Counter()))
        self.assertEqual(count, 1)
        self.assertNotIn("id", candidate["payload"])
        self.assertEqual(candidate["payload"]["call_id"], "call_stable")


if __name__ == "__main__":
    unittest.main(verbosity=2)
