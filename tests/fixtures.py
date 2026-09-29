"""Small synthetic Codex homes; never use a real user session for mutation tests."""
import json
from pathlib import Path
import sqlite3
import time
import uuid

ROOT_ID = "11111111-1111-4111-8111-111111111111"
CHILD_ID = "22222222-2222-4222-8222-222222222222"
GRANDCHILD_ID = "33333333-3333-4333-8333-333333333333"
SPLIT_ID = "44444444-4444-4444-8444-444444444444"
OTHER_ID = "55555555-5555-4555-8555-555555555555"


def create_home(home):
    home = Path(home)
    (home / "sessions").mkdir(parents=True)
    state = sqlite3.connect(home / "state_5.sqlite")
    state.executescript("""
    CREATE TABLE threads(id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL, title TEXT, name TEXT, cwd TEXT,
      model TEXT, model_provider TEXT, updated_at INTEGER, created_at INTEGER, preview TEXT, first_user_message TEXT,
      archived INTEGER DEFAULT 0, history_mode TEXT, source TEXT, agent_path TEXT, agent_nickname TEXT, agent_role TEXT);
    CREATE TABLE thread_spawn_edges(parent_thread_id TEXT, child_thread_id TEXT PRIMARY KEY, status TEXT);
    """)
    state.close()
    history = sqlite3.connect(home / "thread_history_1.sqlite")
    history.executescript("""
    CREATE TABLE thread_history_projection_state(thread_id TEXT PRIMARY KEY, next_rollout_byte_offset INTEGER NOT NULL, next_rollout_ordinal INTEGER NOT NULL);
    CREATE TABLE thread_turns(thread_id TEXT,turn_id TEXT,rollout_ordinal INTEGER,status TEXT,error_json TEXT,started_at INTEGER,
      completed_at INTEGER,duration_ms INTEGER,first_user_item_id TEXT,final_agent_item_id TEXT,rollout_byte_offset INTEGER,
      rollout_end_ordinal INTEGER,rollout_end_byte_offset INTEGER,PRIMARY KEY(thread_id,turn_id),UNIQUE(thread_id,rollout_ordinal));
    CREATE TABLE thread_items(thread_id TEXT,turn_id TEXT,item_id TEXT,rollout_ordinal INTEGER,created_at_ms INTEGER,
      item_json TEXT,item_type TEXT,updated_at_ordinal INTEGER,PRIMARY KEY(thread_id,turn_id,item_id));
    CREATE TABLE thread_realtime_items(thread_id TEXT,item_id TEXT,rollout_ordinal INTEGER,created_at_ms INTEGER,
      item_type TEXT,item_json TEXT,PRIMARY KEY(thread_id,item_id));
    """)
    history.close()
    (home / "session_index.jsonl").write_text("", encoding="utf-8")
    return home


def add_thread(home, identity, title, parent=None, segments=1, encrypted=True, nested=True, failed=True, updated=None):
    home = Path(home)
    updated = updated or int(time.time())
    source = {"subagent": {"thread_spawn": {"parent_thread_id": parent, "agent_path": "/root/checker", "depth": 1}}} if parent else "cli"
    history = sqlite3.connect(home / "thread_history_1.sqlite")
    paths, base, start = [], None, 0
    for number in range(segments):
        physical = identity if number == 0 else str(uuid.uuid5(uuid.NAMESPACE_URL, identity + str(number)))
        suffix = identity if number == 0 else identity + "_" + physical
        path = home / "sessions" / ("rollout-2026-09-18T08-00-00-" + suffix + ".jsonl")
        turn = str(uuid.uuid5(uuid.NAMESPACE_URL, physical + ":turn"))
        user, reasoning, answer = physical + ":user", physical + ":reasoning", physical + ":answer"
        meta = {"id": identity, "source": source, "cwd": "C:\\DemoProject"}
        if parent:
            meta["forked_from_id"] = parent
        if base:
            meta["history_base"] = base
        payloads = [
            ("session_meta", meta),
            ("event_msg", {"type": "task_started", "turn_id": turn}),
            ("response_item", {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "帮我检查这份分析结果。"}]}),
            ("event_msg", {"type": "item_completed", "turn_id": turn, "item": {"type": "UserMessage", "id": user, "content": [{"type": "text", "text": "帮我检查这份分析结果。"}]}}),
        ]
        if encrypted:
            payloads.append(("response_item", {"type": "reasoning", "id": reasoning,
                             "summary": [{"type": "summary_text", "text": "保留这段可读的推理摘要。"}], "encrypted_content": "TEST-SECRET-CIPHER-REASONING"}))
        payloads.append(("event_msg", {"type": "item_completed", "turn_id": turn,
                         "item": {"type": "Reasoning", "id": reasoning, "summary_text": ["保留这段可读的推理摘要。"], "raw_content": []}}))
        payloads.append(("response_item", {"type": "agent_message", "id": physical + ":agent", "author": "/root/reviewer", "recipient": "/root",
                         "content": [{"type": "input_text", "text": "子代理的可读检查结论。"}] + ([{"type": "encrypted_content", "encrypted_content": "TEST-SECRET-CIPHER-AGENT"}] if encrypted else [])}))
        if nested:
            payloads.append(("compacted", {"message": "完整保留的明文压缩摘要。", "replacement_history": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "明文摘要。"}]}],
                "guardian_history": [{"type": "reasoning", "id": reasoning, "summary": [], "encrypted_content": "TEST-SECRET-CIPHER-NESTED"}] if encrypted else []}))
        payloads.extend([
            ("response_item", {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "这是原有的可读分析结果，不应丢失。"}]}),
            ("event_msg", {"type": "item_completed", "turn_id": turn, "item": {"type": "AgentMessage", "id": answer, "text": "这是原有的可读分析结果，不应丢失。"}}),
            ("event_msg", {"type": "task_complete", "turn_id": turn}),
        ])
        records = [{"timestamp": "2026-09-18T08:00:00Z", "ordinal": start + index, "type": kind, "payload": payload}
                   for index, (kind, payload) in enumerate(payloads)]
        lines = [(json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8") for row in records]
        offsets = [0]
        for raw in lines:
            offsets.append(offsets[-1] + len(raw))
        path.write_bytes(b"".join(lines))
        status = "failed" if failed else "completed"
        error = json.dumps({"message": "bad response status code 400", "code": "invalid_encrypted_content"}) if failed else None
        history.execute("INSERT INTO thread_history_projection_state VALUES (?,?,?)", (physical, offsets[-1], start + len(records)))
        history.execute("INSERT INTO thread_turns VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (physical, turn, start + 1, status, error, updated, updated + 3, 3000, user, answer, offsets[1], records[-1]["ordinal"], offsets[-1]))
        for row in records:
            payload = row["payload"]
            if payload.get("type") == "item_completed":
                item = payload["item"]
                history.execute("INSERT INTO thread_items VALUES (?,?,?,?,?,?,?,?)", (physical, turn, item["id"], row["ordinal"], updated * 1000,
                                json.dumps(item, ensure_ascii=False), item["type"][0].lower() + item["type"][1:], row["ordinal"]))
        paths.append(path)
        start += len(records)
        base = {"thread_id": physical, "end_ordinal_exclusive": start, "end_byte_offset": offsets[-1]}
    history.commit()
    history.close()
    state = sqlite3.connect(home / "state_5.sqlite")
    state.execute("INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (identity, str(paths[-1]), title, None, "C:\\DemoProject",
                  "gpt-demo", "local-fixture", updated, updated - 800, "仅用于测试的合成会话数据。", "帮我检查这份分析结果。", 0, "paginated",
                  json.dumps(source) if isinstance(source, dict) else source, "/root/checker" if parent else None, "Reviewer" if parent else None, None))
    if parent:
        state.execute("INSERT INTO thread_spawn_edges VALUES (?,?,?)", (parent, identity, "closed"))
    state.commit()
    state.close()
    with (home / "session_index.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"id": identity, "thread_name": title}, ensure_ascii=False) + "\n")
    return paths


def demo_home(home):
    home = create_home(home)
    add_thread(home, ROOT_ID, "检查数据分析工作流", updated=1789713200)
    add_thread(home, CHILD_ID, "核对数据清洗和处理步骤", parent=ROOT_ID, updated=1789713190)
    add_thread(home, GRANDCHILD_ID, "复核汇总结果与边界情况", parent=CHILD_ID, encrypted=False, failed=False, updated=1789713180)
    add_thread(home, SPLIT_ID, "多轮回滚后的研究会话", segments=3, updated=1789713100)
    add_thread(home, OTHER_ID, "整理项目文档和交接清单", encrypted=False, failed=False, updated=1789713000)
    return home
