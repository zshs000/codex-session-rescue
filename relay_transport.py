"""HTTP/SSE transport using the current proxy, without controlling proxy clients."""
import http.client
import json
import re
from urllib.parse import urlsplit, urlunsplit
from urllib.request import getproxies, proxy_bypass

import repair_encrypted_session as engine
from session_gateway import HOP_HEADERS


def validate_network(value):
    engine.require(isinstance(value, str), "网络出口格式无效。")
    if value in {"auto", "direct"}:
        return value
    parsed = urlsplit(value)
    engine.require(parsed.scheme == "http" and parsed.hostname and not parsed.username and
                   not parsed.password and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment,
                   "代理请填写 HTTP 地址，例如 http://127.0.0.1:7897。")
    try:
        parsed.port
    except ValueError as error:
        raise engine.RepairError("代理端口无效。") from error
    return value.rstrip("/")


def selected_proxy(upstream, network):
    validate_network(network)
    if network == "direct":
        return None
    if network != "auto":
        return network
    if upstream.hostname in {"127.0.0.1", "localhost", "::1"} or proxy_bypass(upstream.hostname):
        return None
    value = getproxies().get(upstream.scheme)
    if value:
        validate_network(value)
    return value


def open_response(method, url, headers, raw, network="auto"):
    upstream = urlsplit(url)
    path = urlunsplit(("", "", upstream.path or "/", upstream.query, ""))
    proxy_value = selected_proxy(upstream, network)
    if proxy_value:
        proxy = urlsplit(proxy_value)
        if upstream.scheme == "https":
            connection = http.client.HTTPSConnection(proxy.hostname, proxy.port or 80, timeout=300)
            connection.set_tunnel(upstream.hostname, upstream.port or 443)
        else:
            connection = http.client.HTTPConnection(proxy.hostname, proxy.port or 80, timeout=300)
            path = url
    else:
        client = http.client.HTTPSConnection if upstream.scheme == "https" else http.client.HTTPConnection
        connection = client(upstream.hostname, upstream.port, timeout=300)
    try:
        connection.request(method, path, raw, headers)
        return connection, connection.getresponse()
    except BaseException:
        connection.close()
        raise


def incoming_headers(handler):
    hop = HOP_HEADERS | {key.strip().lower() for key in handler.headers.get("Connection", "").split(",")}
    headers = {key: value for key, value in handler.headers.items()
               if key.lower() not in hop | {"x-session-token"}}
    headers["Accept-Encoding"] = "identity"
    return headers


def send_response_headers(handler, response):
    handler.send_response(response.status)
    hop = HOP_HEADERS | {key.strip().lower() for key in response.getheader("Connection", "").split(",")}
    for key, value in response.getheaders():
        if key.lower() not in hop:
            handler.send_header(key, value)
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.close_connection = True


class ResponseObservation:
    """Inspect protocol metadata while forwarding bytes unchanged; retain no text."""
    MAX_EVENT = 1024 * 1024

    def __init__(self, response, expect_stream=False):
        self.is_sse = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() == "text/event-stream"
        self.expect_stream = expect_stream
        self.bytes_received = self.bytes_forwarded = self.event_count = 0
        self.last_event = self.terminal_event = self.response_id = self.error_code = ""
        self.failure_stage = ""
        self.malformed_events = self.oversized_events = 0
        self._pending = b""
        self._data = []
        self._event = ""
        self._size = 0
        self._skip_line = self._skip_event = False

    @staticmethod
    def identifier(value):
        return value[:128] if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) else ""

    def feed(self, chunk):
        self.bytes_received += len(chunk)
        if not self.is_sse:
            return
        self._pending += chunk
        while True:
            match = re.search(br"\r\n|\r|\n", self._pending)
            if not match or (match.group() == b"\r" and match.end() == len(self._pending)):
                break
            line, self._pending = self._pending[:match.start()], self._pending[match.end():]
            if self._skip_line:
                self._skip_line = False
            else:
                self._line(line)
        if len(self._pending) > self.MAX_EVENT:
            self._pending = b""
            self._skip_line = self._skip_event = True
            self._data = []
            self.oversized_events += 1

    def _line(self, line):
        if not line:
            self._dispatch()
            self._data, self._event, self._size, self._skip_event = [], "", 0, False
            return
        if self._skip_event or line.startswith(b":"):
            return
        field, separator, value = line.partition(b":")
        if value.startswith(b" "):
            value = value[1:]
        if field == b"event":
            self._event = self.identifier(value.decode("utf-8", errors="replace"))
        elif field == b"data":
            self._size += len(value)
            if self._size > self.MAX_EVENT:
                self._skip_event = True
                self._data = []
                self.oversized_events += 1
            else:
                self._data.append(value)

    def _dispatch(self):
        if self._skip_event or not self._data:
            return
        raw = b"\n".join(self._data)
        self.event_count += 1
        if raw.strip() == b"[DONE]":
            self.last_event = "[DONE]"
            return
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeError):
            self.malformed_events += 1
            self.last_event = self._event or "invalid_json"
            return
        if not isinstance(payload, dict):
            self.malformed_events += 1
            return
        kind = self.identifier(payload.get("type")) or self._event
        self.last_event = kind or "unknown"
        response = payload.get("response")
        response = response if isinstance(response, dict) else {}
        self.response_id = self.identifier(response.get("id")) or self.response_id
        if kind in {"response.completed", "response.failed", "response.incomplete", "error"}:
            # A later completed event must not erase an earlier protocol failure.
            if not self.terminal_event or self.terminal_event == "response.completed":
                self.terminal_event = kind
            details = response.get("error") or response.get("incomplete_details") or payload.get("error") or payload
            if isinstance(details, dict):
                self.error_code = self.identifier(details.get("code") or details.get("reason")) or self.error_code

    def outcome(self):
        if not self.is_sse:
            return ("unexpected_content_type", "流请求未收到 SSE 响应") if self.expect_stream else ("http_complete", "")
        if self.terminal_event == "response.completed":
            return "completed", ""
        messages = {"response.failed": "上游返回 response.failed", "response.incomplete": "上游返回 response.incomplete",
                    "error": "上游返回流错误事件"}
        if self.terminal_event in messages:
            return self.terminal_event, messages[self.terminal_event]
        return "missing_completed", "流提前结束：未收到 response.completed"

    def metadata(self):
        return {key: getattr(self, key) for key in ("is_sse", "bytes_received", "bytes_forwarded", "event_count",
                "last_event", "terminal_event", "response_id", "error_code", "failure_stage", "malformed_events", "oversized_events")}


def send_response_body(handler, response, observation=None):
    while True:
        if observation:
            observation.failure_stage = "upstream_read"
        chunk = response.read1(64 * 1024)
        if not chunk:
            if response.length not in (None, 0):
                raise http.client.IncompleteRead(b"", response.length)
            if observation:
                observation.failure_stage = ""
            return
        if observation:
            observation.feed(chunk)
            observation.failure_stage = "downstream_write"
        handler.wfile.write(chunk)
        handler.wfile.flush()
        if observation:
            observation.bytes_forwarded += len(chunk)
