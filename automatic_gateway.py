"""Provider-wide automatic ID filtering; persistent counters, incremental replies."""
from collections import Counter, deque
import copy
import http.client
import json
from pathlib import Path
import threading
import time
from urllib.parse import urlsplit

import repair_encrypted_session as engine
from provider_config import patch_provider, read_config, snapshot_fields, write_config
from relay_transport import incoming_headers, open_response, send_response_body, send_response_headers, validate_network
from session_gateway import DROP, MAX_BODY, filter_input_item, upstream_url

PREFIX = "/auto/v1"
DETACHABLE = {"message", "reasoning", "function_call", "function_call_output",
              "custom_tool_call", "custom_tool_call_output", "compaction", "compaction_summary"}


def clean_request(body, ids=True, encrypted=False):
    engine.require(isinstance(body, dict), "Responses 请求必须是 JSON 对象。")
    if not ids and not encrypted:
        return body, {"ids": 0, "encrypted": 0, "types": {}}
    engine.require(not body.get("previous_response_id") and not body.get("conversation"),
                   "请求依赖服务端历史引用，未删除引用；请用携带完整历史的 HTTP 会话接入。")
    result, counts, types = dict(body), {"removed": 0}, Counter()
    items = body.get("input")
    if isinstance(items, list):
        cleaned = []
        for item in items:
            engine.require(not isinstance(item, dict) or item.get("type") != "item_reference",
                           "请求只有服务端 item 引用，缺少对应历史内容，已停止转发。")
            value = item
            if ids and isinstance(value, dict) and value.get("type") in DETACHABLE and "id" in value:
                types[value["type"]] += 1
                value = {key: child for key, child in value.items() if key != "id"}
            if encrypted:
                value = filter_input_item(value, counts)
            if value is not DROP:
                cleaned.append(value)
        result["input"] = cleaned
    elif items is not None:
        engine.require(isinstance(items, str), "Responses input 格式尚不支持。")
    return result, {"ids": sum(types.values()), "encrypted": counts["removed"], "types": dict(types)}


def fresh_counters():
    return {"requests": 0, "ids": 0, "encrypted": 0, "changed": 0, "errors": 0, "types": {}, "reset_at": time.time()}


class AutomaticGateway:
    def __init__(self, home, data_dir):
        self.home, self.data_dir = Path(home), Path(data_dir)
        self.file = self.data_dir / "automatic-gateway.json"
        self.lock = threading.RLock()
        self.active = 0
        self.generation = 0
        self.events = deque(maxlen=24)
        self.settings = {"enabled": False, "filter_ids": True, "filter_encrypted": False, "network": "auto",
                         "provider_id": "", "upstream_url": "", "configured_url": "", "original": {}}
        self.counters = fresh_counters()
        if self.file.exists():
            saved = engine.read_json(self.file)
            self.settings.update(saved.get("settings", {}))
            self.counters.update(saved.get("counters", {}))
            self.events.extend(saved.get("events", [])[:24])

    def save(self):
        engine.save_json(self.file, {"settings": self.settings, "counters": self.counters, "events": list(self.events)})

    def defaults(self):
        try:
            _, config, provider = read_config(self.home)
            return {"provider_id": provider, "upstream_url": config["model_providers"][provider].get("base_url", "")}
        except (OSError, ValueError, engine.RepairError):
            return {"provider_id": "", "upstream_url": ""}

    def public(self, origin):
        with self.lock:
            result = copy.deepcopy(self.settings)
            result.pop("original", None)
            defaults = self.defaults()
            attached = (result["enabled"] and defaults["provider_id"] == result["provider_id"] and
                        defaults["upstream_url"].rstrip("/") == origin + PREFIX)
            if not result["enabled"]:
                result.update(defaults)
            return {**result, "proxy_url": origin + PREFIX, "attached": attached, "active": self.active,
                    "counters": copy.deepcopy(self.counters), "events": list(self.events)}

    def configure(self, origin, options):
        with self.lock:
            network = validate_network(options.get("network", self.settings["network"]))
            ids = options.get("filter_ids", self.settings["filter_ids"])
            encrypted = options.get("filter_encrypted", self.settings["filter_encrypted"])
            engine.require(type(ids) is bool and type(encrypted) is bool, "过滤开关格式无效。")
            before, config, provider = read_config(self.home)
            engine.require(not self.settings["enabled"] or provider == self.settings["provider_id"],
                           "当前厂商已变更，请先恢复原厂商直连，再接入新厂商。")
            section = config["model_providers"][provider]
            current = section.get("base_url", "").rstrip("/")
            owned = (provider == self.settings["provider_id"] and current == self.settings["configured_url"])
            upstream = upstream_url(options.get("upstream_url") or (self.settings["upstream_url"] if owned else current))
            target = origin + PREFIX
            engine.require(urlsplit(upstream).netloc != urlsplit(origin).netloc,
                           "上游地址不能指向本工具。")
            engine.require(section.get("wire_api", "responses") == "responses", "自动守护支持 Responses 厂商。")
            original = self.settings["original"] if owned else snapshot_fields(section)
            after = patch_provider(before, provider, {"base_url": {"present": True, "value": target},
                                                      "supports_websockets": {"present": True, "value": False}})
            old_settings = copy.deepcopy(self.settings)
            self.settings.update(enabled=True, filter_ids=ids, filter_encrypted=encrypted, network=network,
                                 provider_id=provider, upstream_url=upstream, configured_url=target, original=original)
            # Save recovery information before the config edit.
            self.save()
            try:
                write_config(self.home, self.data_dir, before, after)
            except BaseException:
                self.settings = old_settings
                self.save()
                raise
            return self.public(origin)

    def reconnect(self, origin):
        """Only update a port/address still owned by this tool."""
        with self.lock:
            if not self.settings["enabled"]:
                return
            defaults = self.defaults()
            if (defaults["provider_id"] == self.settings["provider_id"] and
                    defaults["upstream_url"].rstrip("/") == self.settings["configured_url"]):
                self.configure(origin, {})

    def disconnect(self, origin):
        with self.lock:
            if self.settings["enabled"]:
                before, config, _ = read_config(self.home, require_provider=False)
                provider = self.settings["provider_id"]
                section = config["model_providers"].get(provider, {})
                owned = {"base_url": self.settings["configured_url"], "supports_websockets": False}
                changes = {key: saved for key, saved in self.settings["original"].items()
                           if key in section and section[key] == owned[key]}
                if changes:
                    write_config(self.home, self.data_dir, before, patch_provider(before, provider, changes))
                self.settings["enabled"] = False
                self.save()
            return self.public(origin)

    def reset(self, origin):
        with self.lock:
            self.generation += 1
            self.counters = fresh_counters()
            self.events.clear()
            self.save()
            return self.public(origin)

    def begin(self):
        with self.lock:
            self.active += 1
            self.counters["requests"] += 1
            self.save()
            return self.generation, copy.deepcopy(self.settings)

    def count(self, generation, counts):
        with self.lock:
            if generation != self.generation:
                return
            self.counters["ids"] += counts["ids"]
            self.counters["encrypted"] += counts["encrypted"]
            self.counters["changed"] += int(bool(counts["ids"] or counts["encrypted"]))
            for kind, number in counts["types"].items():
                self.counters["types"][kind] = self.counters["types"].get(kind, 0) + number
            self.save()

    def end(self, generation, counts, status, started, message=""):
        with self.lock:
            self.active -= 1
            if generation != self.generation:
                return
            self.counters["errors"] += int(status >= 400 or bool(message))
            self.events.appendleft({"time": time.time(), "ids": counts["ids"], "encrypted": counts["encrypted"],
                                    "status": status, "seconds": round(time.monotonic() - started, 2), "message": message})
            self.save()

    def relay(self, handler):
        parsed = urlsplit(handler.path)
        suffix = parsed.path.removeprefix(PREFIX)
        if suffix not in {"/responses", "/responses/compact", "/models"} or (
                handler.command != ("GET" if suffix == "/models" else "POST")):
            handler.reject("自动守护支持 HTTP Responses 和 models；不支持 WebSocket。", 400)
            return
        generation, settings = self.begin()
        connection, sent = None, False
        counts = {"ids": 0, "encrypted": 0, "types": {}}
        status, message, started = 502, "", time.monotonic()
        try:
            engine.require(settings["enabled"], "自动守护尚未接入，请在页面开启。")
            raw = None
            if handler.command == "POST":
                engine.require(not handler.headers.get("Transfer-Encoding"), "请使用带 Content-Length 的请求。")
                length = int(handler.headers.get("Content-Length", "0"))
                engine.require(0 < length <= MAX_BODY, "请求体大小无效。")
                engine.require(handler.headers.get_content_type() == "application/json", "请求必须是 JSON。")
                raw = handler.rfile.read(length)
                engine.require(len(raw) == length, "请求体没有完整接收。")
                body = json.loads(raw)
                filtered, counts = clean_request(body, settings["filter_ids"], settings["filter_encrypted"])
                if filtered != body:
                    raw = json.dumps(filtered, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.count(generation, counts)
            url = settings["upstream_url"].rstrip("/") + suffix
            if parsed.query:
                url += "?" + parsed.query
            connection, response = open_response(handler.command, url, incoming_headers(handler), raw, settings["network"])
            status = response.status
            send_response_headers(handler, response)
            sent = True
            send_response_body(handler, response)
        except (BrokenPipeError, ConnectionResetError):
            message = "连接已中断"
        except (engine.RepairError, ValueError) as error:
            status, message = 400, str(error) if isinstance(error, engine.RepairError) else "请求 JSON 或长度无效。"
            if not sent:
                handler.reject(message, status)
        except (OSError, http.client.HTTPException) as error:
            status, message = 502, "上游连接失败：" + type(error).__name__
            if not sent:
                handler.reject(message, status)
        finally:
            if connection:
                connection.close()
            self.end(generation, counts, status, started, message)
