"""Read-only session discovery. No credentials or encrypted values are exposed."""
from __future__ import annotations

from collections import defaultdict, deque
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

import repair_encrypted_session as engine
import repair_split_encrypted_session as split

UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


def safe_text(value, limit=180):
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", str(value or ""))[:limit]


def physical_id(path):
    found = UUID_PATTERN.findall(Path(path).stem)
    return found[-1].lower() if found else None


class SessionCatalog:
    def __init__(self, home):
        self.home = engine.normalized_path(home)
        self.lock = threading.RLock()
        self.loaded_at = 0
        self.rows = {}
        self.parents = {}
        self.children = defaultdict(list)
        self.error_ids = set()

    def refresh(self, force=False):
        with self.lock:
            if not force and time.monotonic() - self.loaded_at < 4:
                return
            con = engine.connect_readonly(self.home / "state_5.sqlite")
            try:
                columns = {r[1] for r in con.execute("PRAGMA table_info(threads)")}
                wanted = [name for name in ("id", "rollout_path", "title", "name", "cwd", "model", "model_provider",
                          "updated_at", "created_at", "preview", "first_user_message", "archived", "history_mode",
                          "agent_path", "agent_nickname", "agent_role", "source") if name in columns]
                rows = {r["id"]: dict(r) for r in con.execute("SELECT " + ",".join(wanted) + " FROM threads")}
                try:
                    edges = [dict(r) for r in con.execute("SELECT parent_thread_id,child_thread_id,status FROM thread_spawn_edges")]
                except sqlite3.OperationalError:
                    edges = []
            finally:
                con.close()
            parents = {e["child_thread_id"]: e["parent_thread_id"] for e in edges}
            for identity, row in rows.items():
                source = row.get("source")
                try:
                    source = json.loads(source) if isinstance(source, str) and source.startswith("{") else {}
                    spawn = source.get("subagent", {}).get("thread_spawn", {})
                    if spawn.get("parent_thread_id"):
                        parents.setdefault(identity, spawn["parent_thread_id"])
                except (ValueError, AttributeError):
                    pass
            children = defaultdict(list)
            for child, parent in parents.items():
                children[parent].append(child)
            for child_list in children.values():
                child_list.sort(key=lambda key: rows.get(key, {}).get("created_at", 0))
            error_ids = set()
            try:
                history = engine.connect_readonly(self.home / "thread_history_1.sqlite")
                try:
                    error_ids = {r[0] for r in history.execute(
                        "SELECT DISTINCT thread_id FROM thread_turns WHERE instr(error_json,'invalid_encrypted_content')>0"
                    )}
                finally:
                    history.close()
            except (sqlite3.Error, OSError):
                pass
            self.rows, self.parents, self.children, self.error_ids = rows, parents, children, error_ids
            self.loaded_at = time.monotonic()

    def public(self, row):
        identity = row["id"]
        path = safe_text(row.get("rollout_path"), 1200)
        alias = physical_id(path)
        title = row.get("name") or row.get("title") or row.get("agent_path") or "未命名会话"
        return {
            "id": identity, "title": safe_text(title, 140),
            "preview": safe_text(row.get("preview") or row.get("first_user_message"), 220),
            "cwd": safe_text(row.get("cwd"), 800).removeprefix("\\\\?\\"),
            "model": safe_text(row.get("model"), 80), "updated_at": row.get("updated_at") or 0,
            "created_at": row.get("created_at") or 0, "archived": bool(row.get("archived")),
            "is_subagent": identity in self.parents, "parent_id": self.parents.get(identity),
            "agent_path": safe_text(row.get("agent_path"), 160),
            "agent_nickname": safe_text(row.get("agent_nickname"), 100),
            "child_count": len(self.children.get(identity, [])),
            "has_error_record": identity in self.error_ids or alias in self.error_ids,
        }

    def search(self, query="", kind="main", archived=False, offset=0, limit=40):
        self.refresh()
        match = UUID_PATTERN.search(query)
        query = match.group().lower() if match else query.strip().casefold()
        with self.lock:
            found = []
            for row in self.rows.values():
                public = self.public(row)
                if not match:
                    if not archived and public["archived"]:
                        continue
                    if kind == "main" and public["is_subagent"]:
                        continue
                    if kind == "agents" and not public["is_subagent"]:
                        continue
                    if kind == "errors" and not public["has_error_record"]:
                        continue
                haystack = " ".join(str(row.get(key) or "") for key in ("id", "title", "name", "cwd", "preview", "first_user_message", "agent_path", "agent_nickname")).casefold()
                if query and (row["id"].lower() != query if match else query not in haystack):
                    continue
                found.append(public)
            found.sort(key=lambda r: r["updated_at"], reverse=True)
            return {"items": found[offset:offset + limit], "total": len(found),
                    "offset": offset, "limit": limit, "all_count": len(self.rows)}

    def get(self, identity):
        self.refresh()
        with self.lock:
            if identity not in self.rows:
                raise engine.RepairError("没有找到这条会话，请确认 ID 或数据目录。")
            return self.public(self.rows[identity])

    def family(self, identity):
        root = self.get(identity)
        with self.lock:
            result, seen, queue = [], set(), deque([(identity, 0)])
            while queue:
                current, depth = queue.popleft()
                if current in seen:
                    continue
                seen.add(current)
                row = self.rows.get(current)
                if row:
                    result.append({**self.public(row), "depth": depth})
                for child in self.children.get(current, []):
                    queue.append((child, depth + 1))
            parent = self.rows.get(self.parents.get(identity))
            return {"thread": root, "parent": self.public(parent) if parent else None, "nodes": result}

    def source(self, identity):
        self.get(identity)
        with self.lock:
            path = engine.normalized_path(self.rows[identity]["rollout_path"])
        engine.require(any(path.is_relative_to(self.home / folder) for folder in ("sessions", "archived_sessions")),
                       "会话文件不在当前数据目录内，已停止处理。")
        return path

    def error_info(self, identity):
        alias = physical_id(self.source(identity))
        con = engine.connect_readonly(self.home / "thread_history_1.sqlite")
        try:
            rows = list(con.execute("SELECT status,error_json,completed_at FROM thread_turns WHERE thread_id=? ORDER BY rollout_ordinal DESC LIMIT 12", (alias,)))
        finally:
            con.close()
        for index, row in enumerate(rows):
            if "invalid_encrypted_content" in (row["error_json"] or ""):
                return {"found": True, "latest_failed": index == 0 and row["status"] == "failed",
                        "code": "invalid_encrypted_content", "at": row["completed_at"]}
        return {"found": False, "latest_failed": False}


def explain_error(error):
    message = str(error)
    rules = [
        (("database is locked", "database table is locked"), "历史索引正被其他进程使用，请稍后重试。"),
        (("writer lock", "Sharing violation", "Permission denied"), "会话正在被占用。退出这条会话及所选子代理后，重新检查。"),
        (("not at EOF", "not at EOF", "projection is not at EOF"), "历史索引尚未追上文件，暂不能安全清理。请先关闭并重新打开该会话，再检查。"),
        (("Non-contiguous", "not contiguous"), "历史记录的序号不连续，需要先修复结构，工具不会直接删除内容。"),
        (("history base", "shares this history", "shared history", "Another thread"), "其他会话也引用了这段历史，暂不能单独修改，以免影响关联会话。"),
        (("active opaque compaction",), "当前压缩记录只有加密内容，无法确认上下文完整性，本版暂不自动清理。"),
        (("readable content", "Readable reasoning"), "有可读推理摘要尚未找到安全保留副本，已停止自动清理。"),
        (("changed", "stale", "mismatch"), "会话或索引已发生变化，请重新检查；现有内容没有被覆盖。"),
        (("already clean", "already has no"), "这条会话当前没有需要清理的加密内容。"),
        (("missing", "not found", "No such file"), "需要的会话文件或索引缺失，请确认数据目录。"),
        (("Unsupported", "Unknown", "Unexpected", "Unrecognized"), "发现当前版本尚不支持的记录格式，已停止自动修改。"),
    ]
    for needles, explanation in rules:
        if any(needle.casefold() in message.casefold() for needle in needles):
            return explanation
    return safe_text(message, 260) or "检查未完成，请查看本地日志。"
