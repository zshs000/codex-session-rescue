"""Inspection jobs and reversible, journaled repairs of selected local threads."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid
import zipfile

import repair_encrypted_session as engine
import repair_split_encrypted_session as split
from session_catalog import SessionCatalog, explain_error, physical_id, safe_text

LOG = logging.getLogger("codex-session-repair")


def now():
    return datetime.now().astimezone().isoformat()


def tables_hash(tables):
    return engine.digest(engine.canonical(engine.canonical_tables(tables)).encode("utf-8"))


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class Jobs:
    def __init__(self):
        self.lock = threading.RLock()
        self.jobs = {}
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="session-repair")

    def submit(self, kind, function):
        identity = uuid.uuid4().hex
        with self.lock:
            self.jobs[identity] = {"id": identity, "kind": kind, "state": "queued", "progress": 0, "message": "等待处理", "created_at": now()}
        def update(progress, message):
            with self.lock:
                self.jobs[identity].update(progress=round(progress, 3), message=message)
        def run():
            with self.lock:
                self.jobs[identity]["state"] = "running"
            try:
                result = function(update)
            except Exception as error:
                with self.lock:
                    self.jobs[identity].update(state="failed", error=explain_error(error), message="操作未完成", finished_at=now())
            else:
                with self.lock:
                    self.jobs[identity].update(state="succeeded", result=result, progress=1, message="已完成", finished_at=now())
        self.executor.submit(run)
        return self.get(identity)

    def get(self, identity):
        with self.lock:
            engine.require(identity in self.jobs, "操作记录已过期，请重新检查。")
            return dict(self.jobs[identity])

    def busy(self):
        with self.lock:
            return any(job["state"] in {"queued", "running"} for job in self.jobs.values())


class RepairService:
    def __init__(self, home):
        self.home = engine.normalized_path(home)
        self.catalog = SessionCatalog(self.home)
        self.jobs = Jobs()
        self.inspections = {}
        self.lock = threading.RLock()

    def _skeleton(self, identity, entries):
        path = self.catalog.source(identity)
        segments, visited = [], set()
        while True:
            engine.require(path.is_file(), "Rollout file is missing.")
            alias = physical_id(path)
            engine.require(alias and alias not in visited, "Cyclic history base.")
            visited.add(alias)
            with path.open("rb") as stream:
                first = json.loads(stream.readline())
            payload = first.get("payload", {})
            segments.append({"physical_id": alias, "name": path.name, "bytes": path.stat().st_size,
                             "start_ordinal": first.get("ordinal"), "history_base": payload.get("history_base"), "path": str(path)})
            base = payload.get("history_base")
            if not base:
                break
            candidates = entries.get(base["thread_id"], [])
            engine.require(len(candidates) == 1, "Missing or ambiguous history base.")
            path = candidates[0][0]
            engine.require(any(path.is_relative_to(self.home / folder) for folder in ("sessions", "archived_sessions")), "Unexpected history base path.")
        segments.reverse()
        return segments

    def _plan(self, identity, entries):
        segments = self._skeleton(identity, entries)
        aliases = {identity, *(row["physical_id"] for row in segments)}
        engine.require(not any((self.home / "thread-writer-locks" / (key + ".lock")).exists() for key in aliases), "Target thread writer lock exists.")
        files = []
        if len(segments) > 1:
            summary, chain, candidates, before, after, metadata = split.plan(self.home, identity, entries)
            for original in chain:
                candidate = candidates[original["physical_id"]]["candidate"]
                files.append({"path": original["path"], "physical_id": original["physical_id"],
                              "before": original["data"], "after": candidate["data"],
                              "before_records": original["records"], "after_records": candidate["records"]})
        else:
            summary, original, candidate, before, after, metadata = engine.make_plan(self.home, identity, entries)
            files.append({"path": Path(summary["source"]), "physical_id": identity,
                          "before": original.data, "after": candidate.data,
                          "before_records": original.records, "after_records": candidate.records})
        cutoff = {}
        for item in files:
            base = item["before_records"][0]["payload"].get("history_base")
            if base:
                cutoff[base["thread_id"]] = base["end_ordinal_exclusive"]
        counts = {table: sum(row.get("rollout_ordinal", 0) < cutoff.get(row["thread_id"], float("inf")) for row in rows)
                  for table, rows in before.items()}
        for segment, item in zip(segments, files):
            segment.update(encrypted_blocks=engine.encrypted_count(item["before_records"]),
                           records=len(item["before_records"]), repaired_records=len(item["after_records"]))
        descriptor = {"files": [(str(f["path"]), engine.digest(f["before"])) for f in files],
                      "index": tables_hash(before), "metadata": metadata}
        public = {"id": identity, "ready": summary["encrypted_fields_before"] > 0,
                  "encrypted_blocks": summary["encrypted_fields_before"], "segments": segments,
                  "segment_count": len(segments), "visible_turns": counts["thread_turns"], "visible_items": counts["thread_items"],
                  "bytes_before": sum(len(f["before"]) for f in files), "bytes_after": sum(len(f["after"]) for f in files),
                  "removed_records": sum(len(f["before_records"]) - len(f["after_records"]) for f in files),
                  "index_verified": True, "readable_history_preserved": True,
                  "error_evidence": self.catalog.error_info(identity),
                  "reason": "" if summary["encrypted_fields_before"] else "没有发现需要清理的加密内容。"}
        return {"public": public, "files": files, "before": before, "after": after, "metadata": metadata,
                "fingerprint": engine.digest(engine.canonical(descriptor).encode("utf-8"))}

    def inspect(self, identity, include_children=True):
        family = self.catalog.family(identity)
        nodes = family["nodes"] if include_children else family["nodes"][:1]
        engine.require(len(nodes) <= 128, "关联会话较多，请选择一个子代理分支单独检查。")
        def run(update):
            entries = split.catalog(self.home)
            inspection_id = uuid.uuid4().hex
            output, fingerprints = [], {}
            for index, node in enumerate(nodes):
                update(index / max(len(nodes), 1), f"正在检查 {index + 1}/{len(nodes)}：{node['title'][:45]}")
                try:
                    plan = self._plan(node["id"], entries)
                    result = plan["public"]
                    fingerprints[node["id"]] = plan["fingerprint"]
                    del plan
                except Exception as error:
                    LOG.warning("Inspection failed for %s: %s", node["id"], error)
                    try:
                        segments = self._skeleton(node["id"], entries)
                    except Exception:
                        segments = []
                    result = {"id": node["id"], "ready": False, "reason": explain_error(error),
                              "encrypted_blocks": None, "segments": segments, "segment_count": len(segments),
                              "index_verified": False, "error_evidence": {"found": False}}
                output.append({**node, **result})
            result = {"inspection_id": inspection_id, "root_id": identity, "nodes": output,
                      "checked_at": now(), "includes_children": include_children}
            with self.lock:
                self.inspections[inspection_id] = {"created": time.monotonic(), "result": result, "fingerprints": fingerprints}
                for key in list(self.inspections):
                    if time.monotonic() - self.inspections[key]["created"] > 3600:
                        del self.inspections[key]
            return result
        return self.jobs.submit("inspect", run)

    def clean(self, inspection_id, selected):
        engine.require(isinstance(selected, list) and 0 < len(selected) <= 64 and len(set(selected)) == len(selected), "请选择需要清理的会话。")
        with self.lock:
            inspection = self.inspections.get(inspection_id)
        engine.require(inspection is not None and time.monotonic() - inspection["created"] < 1800, "检查已过期，请重新检查。")
        nodes = {n["id"]: n for n in inspection["result"]["nodes"]}
        engine.require(all(key in nodes and nodes[key]["ready"] for key in selected), "所选会话没有通过检查。")
        def run(update):
            entries = split.catalog(self.home)
            plans = []
            for index, identity in enumerate(selected):
                update(.05 + .25 * index / len(selected), f"再次核对 {index + 1}/{len(selected)}")
                plan = self._plan(identity, entries)
                engine.require(plan["fingerprint"] == inspection["fingerprints"].get(identity), "History changed since inspection.")
                plans.append(plan)
            update(.32, "正在保存原文件和索引备份")
            bundle = self._prepare_bundle(plans, nodes)
            del plans
            update(.5, "备份已保存，正在同步文件与索引")
            self._write_bundle(bundle, "apply")
            self.catalog.loaded_at = 0
            update(.95, "核对文件哈希、引用和索引")
            return {"backup": self._public_backup(bundle), "threads": [nodes[key] for key in selected]}
        return self.jobs.submit("clean", run)

    def enable_gateway(self, gateway, identity, provider_id, upstream, include_children=False):
        """Back up and clean the explicitly selected scope before enabling its rule."""
        from session_gateway import upstream_url
        upstream = upstream_url(upstream)
        family = self.catalog.family(identity)
        nodes = family["nodes"] if include_children else family["nodes"][:1]
        engine.require(len(nodes) <= 128, "关联会话较多，请选择一个子代理分支单独开启。")
        engine.require(isinstance(provider_id, str) and provider_id and
                       all(char.isalnum() or char in "_-" for char in provider_id), "厂商配置名称无效。")
        def run(update):
            update(.1, "正在检查当前会话的文件与索引")
            entries, plans = split.catalog(self.home), []
            for index, node in enumerate(nodes):
                update(.1 + .2 * index / len(nodes), f"正在检查 {index + 1}/{len(nodes)}：{node['title'][:45]}")
                plans.append(self._plan(node["id"], entries))
            update(.3, "正在保存所选会话的原文件与索引备份")
            bundle = self._prepare_bundle(plans, {node["id"]: node for node in nodes})
            del plans
            update(.5, "备份已保存，正在清理当前会话")
            self._write_bundle(bundle, "apply")
            self.catalog.loaded_at = 0
            update(.9, "文件与索引校验通过，正在开启代理规则")
            gateway.enable(identity, provider_id, upstream, bundle["folder"].name, include_children)
            return {"thread_id": identity, "backup": self._public_backup(bundle)}
        return self.jobs.submit("enable_gateway", run)

    def _prepare_bundle(self, plans, nodes):
        identity = "tool-" + datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8]
        folder = self.home / "backups_state" / identity
        folder.mkdir(parents=True, exist_ok=False)
        (folder / "before").mkdir()
        (folder / "after").mkdir()
        before, after = {key: [] for key in engine.KEYS}, {key: [] for key in engine.KEYS}
        metadata, roots, files, paths, physicals = {}, [], [], set(), set()
        removed = 0
        for plan in plans:
            public = plan["public"]
            logical = public["id"]
            metadata[logical] = plan["metadata"]
            roots.append({"id": logical, "title": nodes[logical]["title"], "is_subagent": nodes[logical]["is_subagent"],
                          "visible_turns": public["visible_turns"], "visible_items": public["visible_items"]})
            removed += public["encrypted_blocks"]
            for item in plan["files"]:
                engine.require(item["path"] not in paths and item["physical_id"] not in physicals, "Selected threads share physical history.")
                paths.add(item["path"])
                physicals.add(item["physical_id"])
                blob = f"{len(files):03d}-{item['physical_id']}.jsonl"
                engine.write_new(folder / "before" / blob, item["before"])
                engine.write_new(folder / "after" / blob, item["after"])
                files.append({"path": str(item["path"]), "physical_id": item["physical_id"],
                              "before_blob": "before/" + blob, "after_blob": "after/" + blob,
                              "before_sha256": engine.digest(item["before"]), "after_sha256": engine.digest(item["after"]),
                              "before_bytes": len(item["before"]), "after_bytes": len(item["after"]),
                              "before_records": len(item["before_records"]), "after_records": len(item["after_records"])})
            for table in engine.KEYS:
                before[table].extend(plan["before"][table])
                after[table].extend(plan["after"][table])
        engine.save_json(folder / "history-index.before.json", before)
        engine.save_json(folder / "history-index.after.json", after)
        engine.save_json(folder / "directory-metadata.before.json", metadata)
        manifest = {"format": "session-tool-v1", "state": "prepared", "created_at": now(), "codex_home": str(self.home),
                    "backup": str(folder), "roots": roots, "physical_ids": sorted(physicals), "files": files,
                    "encrypted_fields_before": removed, "encrypted_fields_after": 0,
                    "before_index_hash": tables_hash(before), "after_index_hash": tables_hash(after),
                    "before_metadata_hash": engine.digest(engine.canonical(metadata).encode("utf-8"))}
        engine.save_json(folder / "manifest.json", manifest)
        return self._load_bundle(identity)

    def _backup_folder(self, identity):
        engine.require(isinstance(identity, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,180}", identity), "备份编号无效。")
        folder = (self.home / "backups_state" / identity).resolve()
        engine.require(folder.parent == (self.home / "backups_state").resolve(), "备份路径无效。")
        return folder

    def _load_bundle(self, identity):
        folder = self._backup_folder(identity)
        manifest = engine.read_json(folder / "manifest.json")
        engine.require(engine.normalized_path(manifest.get("codex_home", "")) == self.home, "备份属于其他数据目录。")
        kind = manifest.get("format", "standalone")
        metadata = engine.read_json(folder / "directory-metadata.before.json")
        if kind == "session-tool-v1":
            files, roots = manifest["files"], manifest["roots"]
        elif kind == "split-v1":
            files = [{**f, "before_blob": "original/" + Path(f["path"]).name, "after_blob": "repaired/" + Path(f["path"]).name} for f in manifest["files"]]
            metadata = {manifest["session_id"]: metadata}
            roots = [{"id": manifest["session_id"], "title": metadata[manifest["session_id"]]["state_thread"].get("title", "历史会话")}]
        else:
            engine.require(all(k in manifest for k in ("source", "session_id", "before_sha256", "after_sha256")), "不支持此旧版备份格式。")
            files = [{"path": manifest["source"], "physical_id": manifest["session_id"], "before_blob": "original.jsonl", "after_blob": "repaired.jsonl",
                      "before_sha256": manifest["before_sha256"], "after_sha256": manifest["after_sha256"]}]
            metadata = {manifest["session_id"]: metadata}
            roots = [{"id": manifest["session_id"], "title": metadata[manifest["session_id"]]["state_thread"].get("title", "历史会话")}]
        aliases = []
        for item in files:
            path = engine.normalized_path(item["path"])
            engine.require(any(path.is_relative_to(self.home / name) for name in ("sessions", "archived_sessions")), "备份指向了会话目录之外的路径。")
            engine.require(path.suffix == ".jsonl", "备份中的会话格式无效。")
            aliases.append(item["physical_id"])
            for side in ("before", "after"):
                blob = (folder / item[side + "_blob"]).resolve()
                engine.require(blob.is_relative_to(folder), "备份文件越过了备份目录。")
        engine.require(len(set(aliases)) == len(aliases) and len({engine.normalized_path(f["path"]) for f in files}) == len(files), "备份包含重复的历史文件。")
        for alias in aliases + [r["id"] for r in roots]:
            engine.require(str(uuid.UUID(alias)) == alias, "备份中的会话编号无效。")
        before = engine.read_json(folder / "history-index.before.json")
        after = engine.read_json(folder / "history-index.after.json")
        engine.require(set(before) == set(engine.KEYS) == set(after), "备份的索引表不完整。")
        for table in engine.KEYS:
            engine.require(all(row["thread_id"] in aliases for row in before[table] + after[table]), "备份包含无关会话的索引。")
            old = {tuple(row[key] for key in engine.KEYS[table]): row for row in before[table]}
            new = {tuple(row[key] for key in engine.KEYS[table]): row for row in after[table]}
            engine.require(len(old) == len(before[table]) and old.keys() == new.keys() and len(new) == len(after[table]), "备份的历史条目集合不一致。")
            for key in old:
                engine.require(old[key].keys() == new[key].keys() and all(old[key][col] == new[key][col] for col in old[key] if col not in engine.POSITION_COLUMNS), "备份修改了可读历史内容。")
        engine.require(set(metadata) == {row["id"] for row in roots}, "备份的会话元数据不完整。")
        if kind == "session-tool-v1":
            engine.require(tables_hash(before) == manifest["before_index_hash"] and tables_hash(after) == manifest["after_index_hash"], "索引备份校验失败。")
            engine.require(engine.digest(engine.canonical(metadata).encode("utf-8")) == manifest["before_metadata_hash"], "元数据备份校验失败。")
        return {"id": identity, "folder": folder, "manifest": manifest, "files": files, "roots": roots,
                "physical_ids": aliases, "metadata": metadata, "before": before, "after": after}

    def _save_state(self, bundle, state, **extra):
        bundle["manifest"].update(state=state, updated_at=now(), **extra)
        engine.save_json(bundle["folder"] / "manifest.json", bundle["manifest"])

    def _verify_blobs(self, bundle):
        for item in bundle["files"]:
            for side in ("before", "after"):
                engine.require(file_hash(bundle["folder"] / item[side + "_blob"]) == item[side + "_sha256"], "备份文件校验失败，未覆盖会话。")

    def _replace_from_blob(self, bundle, item, side):
        target = engine.normalized_path(item["path"])
        stage = target.with_name(target.name + ".tool-" + uuid.uuid4().hex[:12] + ".tmp")
        try:
            with (bundle["folder"] / item[side + "_blob"]).open("rb") as source, stage.open("xb") as output:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    output.write(block)
                output.flush()
                os.fsync(output.fileno())
            engine.require(file_hash(stage) == item[side + "_sha256"], "临时文件校验失败。")
            os.replace(stage, target)
        finally:
            if stage.exists():
                stage.unlink()

    def _read_indexes(self, con, bundle):
        return split.read_indexes(con, bundle["physical_ids"])

    def _verify_wanted(self, bundle, side):
        self._verify_blobs(bundle)
        con = engine.connect_readonly(self.home / "thread_history_1.sqlite")
        try:
            con.execute("BEGIN")
            actual = self._read_indexes(con, bundle)
        finally:
            con.close()
        engine.require(engine.canonical_tables(actual) == engine.canonical_tables(bundle[side]), "写入后的索引校验失败。")
        parsed = {}
        for item in bundle["files"]:
            path = engine.normalized_path(item["path"])
            engine.require(file_hash(path) == item[side + "_sha256"], "写入后的会话校验失败。")
            data = path.read_bytes()
            first = json.loads(data.split(b"\n", 1)[0])
            segment = split.parse(data, first["payload"]["id"])
            parsed[item["physical_id"]] = segment
            split.validate(segment, {t: [r for r in rows if r["thread_id"] == item["physical_id"]] for t, rows in actual.items()}, item["physical_id"])
            if side == "after":
                engine.require(engine.encrypted_count(segment["records"]) == 0, "修复后仍有加密内容。")
        for segment in parsed.values():
            base = segment["records"][0]["payload"].get("history_base")
            if base:
                engine.require(base["thread_id"] in parsed, "备份缺少关联历史段。")
                parent = parsed[base["thread_id"]]
                engine.require(base["end_byte_offset"] in parent["offsets"] and base["end_ordinal_exclusive"] == parent["start"] + parent["offsets"].index(base["end_byte_offset"]), "历史段引用没有同步。")
        return {"file_hashes": True, "index_positions": True, "history_references": True, "backup_hashes": True}

    def _write_bundle(self, bundle, mode):
        engine.require(mode in {"apply", "restore", "recover"}, "未知操作。")
        wanted_side = "after" if mode == "apply" else "before"
        allowed = {"before"} if mode == "apply" else {"after"} if mode == "restore" else {"before", "after"}
        self._verify_blobs(bundle)
        with ExitStack() as stack:
            for key in sorted(set(bundle["physical_ids"]) | {r["id"] for r in bundle["roots"]}):
                stack.enter_context(engine.thread_guard(self.home, key))
            split.check_references(split.catalog(self.home), {engine.normalized_path(f["path"]) for f in bundle["files"]}, set(bundle["physical_ids"]))
            for key, metadata in bundle["metadata"].items():
                engine.require(engine.directory_metadata(self.home, key) == metadata, "会话已有新内容或设置发生变化，不能覆盖；备份仍可导出。")
            initial_sides = []
            for item in bundle["files"]:
                current = file_hash(engine.normalized_path(item["path"]))
                matches = [side for side in allowed if current == item[side + "_sha256"]]
                engine.require(matches, "会话已有新消息或文件发生变化，不能覆盖；请重新检查，备份仍可导出。")
                initial_sides.append(matches[0])
            con = sqlite3.connect((self.home / "thread_history_1.sqlite").as_uri() + "?mode=rw", uri=True, timeout=10)
            con.row_factory = sqlite3.Row
            changed, initial_tables, committed = [], None, False
            try:
                con.execute("BEGIN IMMEDIATE")
                initial_tables = self._read_indexes(con, bundle)
                current = engine.canonical_tables(initial_tables)
                engine.require(any(current == engine.canonical_tables(bundle[side]) for side in allowed), "索引已更新，不能覆盖回滚；备份仍可导出。")
                self._save_state(bundle, "applying" if mode == "apply" else "restoring", pending_mode=mode)
                engine.update_positions(con, initial_tables, bundle[wanted_side])
                for index, item in enumerate(bundle["files"]):
                    path = engine.normalized_path(item["path"])
                    engine.require(file_hash(path) == item[initial_sides[index] + "_sha256"], "会话文件在写入过程中发生变化。")
                    if file_hash(path) != item[wanted_side + "_sha256"]:
                        self._replace_from_blob(bundle, item, wanted_side)
                        changed.append(index)
                engine.require(engine.canonical_tables(self._read_indexes(con, bundle)) == engine.canonical_tables(bundle[wanted_side]), "索引更新校验失败。")
                con.commit()
                committed = True
            except BaseException as error:
                con.rollback()
                rollback_ok = True
                try:
                    if changed:
                        engine.require(initial_tables is not None and engine.canonical_tables(self._read_indexes(con, bundle)) == engine.canonical_tables(initial_tables), "数据库提交状态不确定。")
                    for index in reversed(changed):
                        item = bundle["files"][index]
                        engine.require(file_hash(engine.normalized_path(item["path"])) == item[wanted_side + "_sha256"], "外部文件更新阻止了自动恢复。")
                        self._replace_from_blob(bundle, item, initial_sides[index])
                except BaseException:
                    rollback_ok = False
                fallback = "failed" if mode == "apply" else "applied" if mode == "restore" else "needs_recovery"
                self._save_state(bundle, fallback if rollback_ok else "needs_recovery", error=explain_error(error), rollback_ok=rollback_ok)
                raise
            finally:
                con.close()
            if committed:
                try:
                    verification = self._verify_wanted(bundle, wanted_side)
                except Exception as error:
                    self._save_state(bundle, "needs_recovery", error=explain_error(error))
                    raise
                final = "applied" if mode == "apply" else "restored" if mode == "restore" else "recovered"
                self._save_state(bundle, final, verification=verification, **{final + "_at": now()})

    def restore(self, identity, recover=False):
        def run(update):
            update(.1, "核对备份和会话当前状态")
            bundle = self._load_bundle(identity)
            update(.4, "正在恢复文件与索引")
            self._write_bundle(bundle, "recover" if recover else "restore")
            self.catalog.loaded_at = 0
            return {"backup": self._public_backup(bundle)}
        return self.jobs.submit("recover" if recover else "restore", run)

    def _public_backup(self, bundle):
        manifest = bundle["manifest"]
        state = manifest.get("state", "prepared")
        return {"id": bundle["id"], "state": state, "created_at": manifest.get("created_at") or manifest.get("prepared_at") or manifest.get("applied_at") or "",
                "updated_at": manifest.get("updated_at") or manifest.get("applied_at") or "",
                "roots": [{"id": r["id"], "title": safe_text(r.get("title"), 140), "is_subagent": bool(r.get("is_subagent"))} for r in bundle["roots"]],
                "segments": len(bundle["files"]), "encrypted_removed": manifest.get("encrypted_fields_before", 0),
                "path": str(bundle["folder"]), "can_restore": state == "applied",
                "can_recover": state in {"prepared", "applying", "restoring", "needs_recovery"},
                "supported": True, "error": safe_text(manifest.get("error"), 260)}

    def backups(self):
        root = self.home / "backups_state"
        result = []
        if not root.exists():
            return {"items": []}
        for folder in root.iterdir():
            if not folder.is_dir() or not (folder / "manifest.json").is_file():
                continue
            try:
                manifest = engine.read_json(folder / "manifest.json")
                if manifest.get("codex_home") and engine.normalized_path(manifest["codex_home"]) == self.home:
                    bundle = self._load_bundle(folder.name)
                    result.append(self._public_backup(bundle))
            except (ValueError, OSError, KeyError, engine.RepairError):
                continue
        result.sort(key=lambda r: r["updated_at"] or r["created_at"] or r["id"], reverse=True)
        return {"items": result}

    def export_backup(self, identity, target):
        bundle = self._load_bundle(identity)
        self._verify_blobs(bundle)
        sources = {"manifest.json", "history-index.before.json", "history-index.after.json", "directory-metadata.before.json"}
        for item in bundle["files"]:
            sources.update((item["before_blob"], item["after_blob"]))
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=3) as archive:
            for name in sorted(sources):
                source = (bundle["folder"] / name).resolve()
                engine.require(source.is_relative_to(bundle["folder"]), "备份路径越界。")
                archive.write(source, name)
