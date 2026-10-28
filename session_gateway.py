"""Local Responses relay with explicit, persistent per-thread filtering rules."""
from __future__ import annotations

import http.client
import json
import logging
import os
from pathlib import Path
import secrets
import threading
import tomllib
from urllib.parse import urlsplit, urlunsplit
import uuid

import repair_encrypted_session as engine
from session_catalog import SessionCatalog
from replay_items import detach_replay_item

LOG = logging.getLogger("codex-session-repair")
MAX_BODY = 64 * 1024 * 1024
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
               "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length"}
DROP = object()


def upstream_url(value):
    engine.require(isinstance(value, str), "请填写这条会话原来的 API URL。")
    parsed = urlsplit(value.strip().rstrip("/"))
    engine.require(parsed.scheme in {"https", "http"} and parsed.hostname and
                   not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,
                   "请填写完整的 HTTP/HTTPS API URL，不含账号、查询参数或片段。")
    try:
        parsed.port
    except ValueError as error:
        raise engine.RepairError("上游 URL 的端口无效。") from error
    engine.require(parsed.scheme == "https" or parsed.hostname in {"127.0.0.1", "localhost", "::1"},
                   "远程上游请使用 HTTPS；HTTP 仅支持本机地址。")
    path = parsed.path.rstrip("/")
    for ending in ("/responses/compact", "/responses"):
        if path.endswith(ending):
            path = path[:-len(ending)]
            break
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def request_thread(headers, body):
    """Agreeing explicit IDs only. Never infer a thread from text or parent IDs."""
    found = set()
    lowered = {key.lower(): value for key, value in headers.items()}

    def add(value):
        if value is None:
            return
        try:
            identity = str(uuid.UUID(value))
        except (ValueError, TypeError, AttributeError) as error:
            raise engine.RepairError("请求中的会话 ID 无效，已停止转发。") from error
        found.add(identity)

    for key in ("thread-id", "session-id"):
        add(lowered.get(key))
    metadata = lowered.get("x-codex-turn-metadata")
    if metadata:
        try:
            metadata = json.loads(metadata)
            engine.require(isinstance(metadata, dict), "请求中的会话元数据格式无效。")
        except ValueError as error:
            raise engine.RepairError("请求中的会话元数据格式无效。") from error
        for key in ("thread_id", "session_id"):
            add(metadata.get(key))
    if isinstance(body, dict):
        metadata = body.get("client_metadata", {})
        if isinstance(metadata, dict):
            for key in ("thread_id", "session_id"):
                add(metadata.get(key))
        # Codex uses the logical thread UUID as the prompt cache key.
        cache_key = body.get("prompt_cache_key")
        if isinstance(cache_key, str):
            try:
                uuid.UUID(cache_key)
            except ValueError:
                pass
            else:
                add(cache_key)
    engine.require(len(found) <= 1, "请求中的会话 ID 相互冲突，已停止转发。")
    return next(iter(found), None)


def filter_input_item(item, counts):
    if not isinstance(item, dict):
        return item
    if item.get("encrypted_content"):
        kind = item.get("type")
        if kind == "reasoning":
            engine.require(set(item) <= {"type", "id", "summary", "content", "encrypted_content",
                                         "status", "internal_chat_message_metadata_passthrough"},
                           "发现尚不支持的加密推理字段，已停止转发。")
            counts["removed"] += 1
            # Keep readable summaries/content; drop only an entirely opaque item.
            result = {key: value for key, value in item.items() if key != "encrypted_content"}
            if not result.get("summary") and not result.get("content"):
                return DROP
            return result
        if kind == "encrypted_content" and set(item) == {"type", "encrypted_content"}:
            counts["removed"] += 1
            return DROP
        if kind in {"compaction", "compaction_summary"}:
            raise engine.RepairError("请求依赖加密压缩记录，删除会丢失上下文；已停止转发。")
        raise engine.RepairError("发现尚不支持的加密输入结构，已停止转发。")
    # Only visit protocol content, never tool arguments/results or user JSON text.
    result = dict(item)
    if isinstance(item.get("content"), list):
        content = []
        for child in item["content"]:
            if isinstance(child, dict) and child.get("type") == "encrypted_content":
                filtered = filter_input_item(child, counts)
                if filtered is not DROP:
                    content.append(filtered)
            else:
                content.append(child)
        result["content"] = content
        engine.require(content or item.get("type") != "message",
                       "消息只包含加密内容，删除会丢失上下文；已停止转发。")
    return result


def filter_request(body):
    engine.require(isinstance(body, dict), "Responses 请求必须是 JSON 对象。")
    engine.require(not body.get("previous_response_id"),
                   "此请求使用服务端历史引用，请通过页面提供的 HTTP 恢复命令重新打开会话。")
    result, counts = dict(body), {"removed": 0, "detached": 0}
    inputs = body.get("input")
    if isinstance(inputs, list):
        result["input"] = []
        for item in inputs:
            filtered = filter_input_item(item, counts)
            if filtered is not DROP:
                filtered, detached = detach_replay_item(filtered)
                counts["detached"] += detached
                result["input"].append(filtered)
    elif inputs is not None:
        engine.require(isinstance(inputs, str), "Responses input 格式尚不支持。")
    # Tools, metadata and ordinary message text remain untouched.
    return result, counts["removed"]


class SessionGateway:
    def __init__(self, home, data_dir):
        self.home = Path(home)
        self.path = Path(data_dir) / "gateway-rules.json"
        self.lock = threading.RLock()
        self.rules = {}
        self.catalog = SessionCatalog(home)
        if self.path.exists():
            saved = engine.read_json(self.path)
            engine.require(saved.get("format") == "session-gateway-v1", "代理规则格式无效。")
            self.rules = saved["rules"]

    def _save(self):
        temporary = self.path.with_suffix(".tmp")
        engine.save_json(temporary, {"format": "session-gateway-v1", "rules": self.rules})
        os.replace(temporary, self.path)

    def defaults(self, identity, catalog):
        catalog.get(identity)
        config_file = self.home / "config.toml"
        config = tomllib.loads(config_file.read_text(encoding="utf-8")) if config_file.exists() else {}
        with catalog.lock:
            provider_id = catalog.rows[identity].get("model_provider") or config.get("model_provider") or "openai"
        provider = config.get("model_providers", {}).get(provider_id, {})
        return {"provider_id": provider_id, "upstream_url": provider.get("base_url", "")}

    def enable(self, identity, provider_id, upstream, backup_id, include_children=False):
        identity = str(uuid.UUID(identity))
        engine.require(isinstance(provider_id, str) and provider_id and
                       all(char.isalnum() or char in "_-" for char in provider_id), "厂商配置名称无效。")
        upstream = upstream_url(upstream)
        engine.require(backup_id, "首次开启代理前必须完成会话备份。")
        with self.lock:
            previous = self.rules.get(identity)
            rule = {"thread_id": identity, "provider_id": provider_id, "upstream_url": upstream,
                    "route": previous["route"] if previous else secrets.token_urlsafe(24),
                    "enabled": True, "include_children": include_children, "backup_id": backup_id,
                    "requests": 0, "filtered_requests": 0,
                    "removed": 0, "last_thread_id": None, "last_error": ""}
            self.rules[identity] = rule
            try:
                self._save()
            except BaseException:
                if previous is None:
                    self.rules.pop(identity, None)
                else:
                    self.rules[identity] = previous
                raise
        return dict(rule)

    def disable(self, identity):
        with self.lock:
            engine.require(identity in self.rules, "这条会话尚未开启代理。")
            self.rules[identity]["enabled"] = False
            self._save()

    def route(self, token):
        with self.lock:
            return next((dict(rule) for rule in self.rules.values() if secrets.compare_digest(rule["route"], token)), None)

    def matches(self, rule, identity):
        if not rule["enabled"] or identity is None:
            return False
        if identity == rule["thread_id"]:
            return True
        if not rule.get("include_children", False):
            return False
        self.catalog.refresh()
        with self.catalog.lock:
            missing = identity not in self.catalog.rows
        if missing:
            self.catalog.refresh(force=True)
        with self.catalog.lock:
            seen, current = set(), identity
            while current not in seen:
                seen.add(current)
                current = self.catalog.parents.get(current)
                if current == rule["thread_id"]:
                    return True
                if current is None:
                    break
        return False

    def public(self, identity, origin):
        with self.lock:
            rule = self.rules.get(identity)
            if not rule:
                return {"enabled": False}
            base = origin + "/gateway/" + rule["route"]
            provider = rule["provider_id"]
            overrides = [f'model_providers.{provider}.base_url={json.dumps(base)}',
                         f'model_providers.{provider}.supports_websockets=false',
                         f'model_providers.{provider}.capabilities.remote_compaction="unsupported"']
            overrides.insert(0, "model_provider=" + json.dumps(provider))
            command = "codex " + " ".join("-c '" + item + "'" for item in overrides) + " resume " + identity
            return {key: rule[key] for key in ("enabled", "upstream_url", "provider_id", "backup_id", "requests",
                     "filtered_requests", "removed", "last_thread_id", "last_error")} | {
                     "proxy_url": base, "resume_command": command,
                     "include_children": rule.get("include_children", False),
                     "connected": rule["filtered_requests"] > 0}

    def record(self, identity, request_id=None, removed=0, matched=False, error=""):
        with self.lock:
            rule = self.rules[identity]
            rule["requests"] += 1
            rule["filtered_requests"] += int(matched)
            rule["removed"] += removed
            rule["last_thread_id"] = request_id
            rule["last_error"] = error
            self._save()

    def relay(self, handler):
        """Relay raw HTTP/SSE responses; never save credentials or message bodies."""
        from relay_transport import open_response
        parsed = urlsplit(handler.path)
        parts = parsed.path.split("/", 3)
        rule = self.route(parts[2]) if len(parts) >= 3 else None
        if not rule:
            handler.reject("没有找到这个代理入口。", 404)
            return
        suffix = "/" + parts[3] if len(parts) == 4 else ""
        if suffix not in {"/responses", "/responses/compact"} or handler.command != "POST":
            handler.reject("代理支持 HTTP POST Responses 请求；请使用页面提供的恢复命令。", 400)
            return
        connection, sent, request_id = None, False, None
        try:
            engine.require(not handler.headers.get("Transfer-Encoding"), "请使用带 Content-Length 的请求。")
            length = int(handler.headers.get("Content-Length", "0"))
            engine.require(0 < length <= MAX_BODY, "请求体大小无效。")
            engine.require(handler.headers.get_content_type() == "application/json", "请求必须是 JSON。")
            raw = handler.rfile.read(length)
            engine.require(len(raw) == length, "请求体没有完整接收。")
            body = json.loads(raw)
            request_id = request_thread(handler.headers, body)
            matched = self.matches(rule, request_id)
            removed = 0
            if matched:
                filtered, removed = filter_request(body)
                if filtered != body:
                    raw = json.dumps(filtered, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            upstream = urlsplit(rule["upstream_url"])
            path = upstream.path.rstrip("/") + suffix
            if parsed.query:
                path += "?" + parsed.query
            # Strip connection-nominated headers as well as standard hop headers.
            hop = HOP_HEADERS | {key.strip().lower() for key in handler.headers.get("Connection", "").split(",")}
            headers = {key: value for key, value in handler.headers.items()
                       if key.lower() not in hop | {"x-session-token"}}
            headers["Accept-Encoding"] = "identity"
            url = rule["upstream_url"].rstrip("/") + suffix
            if parsed.query:
                url += "?" + parsed.query
            connection, response = open_response("POST", url, headers, raw)
            self.record(rule["thread_id"], request_id, removed, matched)
            handler.send_response(response.status)
            response_hop = HOP_HEADERS | {key.strip().lower() for key in response.getheader("Connection", "").split(",")}
            for key, value in response.getheaders():
                if key.lower() not in response_hop:
                    handler.send_header(key, value)
            # HTTP/1.0 close-delimited output supports incremental SSE without buffering.
            handler.send_header("Connection", "close")
            handler.end_headers()
            handler.close_connection = True
            sent = True
            while True:
                chunk = response.read1(64 * 1024)
                if not chunk:
                    break
                handler.wfile.write(chunk)
                handler.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        except (engine.RepairError, ValueError, OSError, http.client.HTTPException) as error:
            # Error details contain no request body or Authorization value.
            message = str(error)[:260]
            self.record(rule["thread_id"], request_id, error=message)
            LOG.warning("Gateway failed for thread %s: %s", rule["thread_id"], message)
            if not sent:
                handler.reject(message, 400 if isinstance(error, (engine.RepairError, ValueError)) else 502)
        finally:
            if connection:
                connection.close()
