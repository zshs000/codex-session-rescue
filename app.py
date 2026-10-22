"""Local-only Codex session repair UI. Python 3.11+, no runtime dependencies."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import secrets
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlsplit
from urllib.request import urlopen
import webbrowser

import repair_encrypted_session as engine
from repair_service import RepairService
from session_catalog import explain_error
from session_gateway import SessionGateway, upstream_url

APP_ID = "codex-session-repair"
VERSION = "1.1.0"
ASSETS = Path(__file__).resolve().parent / "web"
LOG = logging.getLogger(APP_ID)


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, home, data_dir, port=0):
        self.service = RepairService(home)
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.gateway = SessionGateway(home, self.data_dir)
        self.token = secrets.token_urlsafe(32)
        self.nonce = secrets.token_hex(16)
        super().__init__(("127.0.0.1", port), Handler)
        self.url = f"http://127.0.0.1:{self.server_address[1]}"
        self.allowed_hosts = {f"127.0.0.1:{self.server_address[1]}", f"localhost:{self.server_address[1]}"}
        self.allowed_origins = {"http://" + host for host in self.allowed_hosts}


class Handler(BaseHTTPRequestHandler):
    server_version = "LocalSessionRepair"

    def log_message(self, format, *args):
        LOG.info("%s %s", self.command, urlsplit(self.path).path)

    def headers_for(self, status, content_type="application/json; charset=utf-8", length=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        if length is not None:
            self.send_header("Content-Length", str(length))

    def send_json(self, value, status=200):
        raw = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.headers_for(status, length=len(raw))
        self.end_headers()
        self.wfile.write(raw)

    def reject(self, message, status=400):
        self.send_json({"error": message}, status)

    def validate_request(self, mutation=False):
        if self.headers.get("Host", "") not in self.server.allowed_hosts:
            self.reject("只允许通过本机地址访问。", 403)
            return False
        origin = self.headers.get("Origin")
        if origin and origin not in self.server.allowed_origins:
            self.reject("请求来源不受支持。", 403)
            return False
        if mutation:
            token = self.headers.get("X-Session-Token", "")
            if origin not in self.server.allowed_origins or not secrets.compare_digest(token, self.server.token):
                self.reject("页面授权已失效，请刷新后重试。", 403)
                return False
        return True

    def do_OPTIONS(self):
        self.reject("不允许跨站调用。", 403)

    def do_GET(self):
        if not self.validate_request():
            return
        parsed = urlsplit(self.path)
        path = parsed.path
        try:
            assets = {"/": ("index.html", "text/html; charset=utf-8"), "/index.html": ("index.html", "text/html; charset=utf-8"),
                      "/app.js": ("app.js", "text/javascript; charset=utf-8"), "/style.css": ("style.css", "text/css; charset=utf-8"),
                      "/icon.svg": ("icon.svg", "image/svg+xml")}
            if path in assets:
                name, mime = assets[path]
                raw = (ASSETS / name).read_bytes()
                self.headers_for(200, mime, len(raw))
                self.end_headers()
                self.wfile.write(raw)
                return
            service = self.server.service
            if path.startswith("/gateway/"):
                self.server.gateway.relay(self)
                return
            if path == "/api/health":
                self.send_json({"app": APP_ID, "version": VERSION, "nonce": self.server.nonce})
            elif path == "/api/bootstrap":
                self.send_json({"app": APP_ID, "version": VERSION, "token": self.server.token,
                                "home": str(service.home), "home_exists": (service.home / "state_5.sqlite").is_file()})
            elif path == "/api/sessions":
                query = parse_qs(parsed.query)
                one = lambda key, default: query.get(key, [default])[0]
                kind = one("kind", "main")
                engine.require(kind in {"main", "all", "agents", "errors"}, "筛选条件无效。")
                offset = max(0, min(int(one("offset", "0")), 100000))
                limit = max(1, min(int(one("limit", "40")), 100))
                self.send_json(service.catalog.search(one("q", "")[:500], kind, one("archived", "false") == "true", offset, limit))
            elif path.startswith("/api/sessions/"):
                identity = path.removeprefix("/api/sessions/")
                self.send_json(service.catalog.family(identity))
            elif path.startswith("/api/gateway/"):
                identity = path.removeprefix("/api/gateway/")
                self.send_json({**self.server.gateway.defaults(identity, service.catalog),
                                **self.server.gateway.public(identity, self.server.url)})
            elif path.startswith("/api/jobs/"):
                self.send_json(service.jobs.get(path.removeprefix("/api/jobs/")))
            elif path == "/api/backups":
                self.send_json(service.backups())
            elif path.startswith("/api/backups/") and path.endswith("/export"):
                token = self.headers.get("X-Session-Token", "")
                if not secrets.compare_digest(token, self.server.token):
                    self.reject("请从工具页面导出备份。", 403)
                    return
                identity = path[len("/api/backups/"):-len("/export")]
                descriptor, temporary = tempfile.mkstemp(prefix="export-", suffix=".zip", dir=self.server.data_dir)
                os.close(descriptor)
                temporary = Path(temporary)
                try:
                    service.export_backup(identity, temporary)
                    self.headers_for(200, "application/zip", temporary.stat().st_size)
                    self.send_header("Content-Disposition", f'attachment; filename="{identity}.zip"')
                    self.end_headers()
                    with temporary.open("rb") as stream:
                        for block in iter(lambda: stream.read(1024 * 1024), b""):
                            self.wfile.write(block)
                finally:
                    temporary.unlink(missing_ok=True)
            else:
                self.reject("没有找到这个页面。", 404)
        except (BrokenPipeError, ConnectionResetError):
            return
        except (engine.RepairError, ValueError, OSError, KeyError) as error:
            LOG.warning("Read failed: %s", error)
            self.reject(explain_error(error), 400)
        except Exception:
            LOG.exception("Read failed")
            self.reject("读取失败，详情已写入本地日志。", 500)

    def do_POST(self):
        if urlsplit(self.path).path.startswith("/gateway/"):
            if self.validate_request():
                self.server.gateway.relay(self)
            return
        if not self.validate_request(mutation=True):
            return
        path = urlsplit(self.path).path
        try:
            length = int(self.headers.get("Content-Length", "0"))
            engine.require(0 < length <= 65536, "请求体大小无效。")
            engine.require(self.headers.get_content_type() == "application/json", "请求必须是 JSON。")
            data = json.loads(self.rfile.read(length))
            engine.require(isinstance(data, dict), "请求内容无效。")
            service = self.server.service
            if path == "/api/inspect":
                engine.require(isinstance(data.get("thread_id"), str), "请选择会话。")
                self.send_json(service.inspect(data["thread_id"], data.get("include_children", True) is True), 202)
            elif path == "/api/clean":
                self.send_json(service.clean(data.get("inspection_id"), data.get("thread_ids")), 202)
            elif path == "/api/gateway/enable":
                upstream = upstream_url(data.get("upstream_url"))
                engine.require(upstream not in self.server.allowed_origins and
                               not upstream.startswith(self.server.url + "/"), "上游 URL 应填写原厂商地址，不能指向本工具。")
                self.send_json(service.enable_gateway(self.server.gateway, data.get("thread_id"),
                                                      data.get("provider_id"), upstream,
                                                      data.get("include_children", False) is True), 202)
            elif path == "/api/gateway/disable":
                self.server.gateway.disable(data.get("thread_id"))
                self.send_json({"enabled": False})
            elif path.startswith("/api/backups/") and path.rsplit("/", 1)[-1] in {"restore", "recover"}:
                identity, action = path[len("/api/backups/"):].rsplit("/", 1)
                self.send_json(service.restore(identity, recover=action == "recover"), 202)
            elif path == "/api/shutdown":
                if service.jobs.busy():
                    self.reject("正在处理会话，完成后再退出工具。", 409)
                else:
                    self.send_json({"stopped": True})
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
            else:
                self.reject("没有找到这个操作。", 404)
        except (engine.RepairError, ValueError, TypeError, KeyError) as error:
            self.reject(explain_error(error), 400)
        except Exception:
            LOG.exception("Operation submission failed")
            self.reject("操作未启动，请刷新后重试。", 500)


@contextmanager
def instance_lock(path):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateFileW.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE)
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel.CreateFileW(str(path), 0xC0000000, 0, None, 4, 0x04000080, None)
        acquired = handle != wintypes.HANDLE(-1).value
        try:
            yield acquired
        finally:
            if acquired:
                kernel.CloseHandle(handle)
    else:
        import fcntl
        with path.open("a+b") as stream:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
            else:
                try:
                    yield True
                finally:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def existing_url(runtime):
    try:
        state = engine.read_json(runtime)
        port = int(state["port"])
        engine.require(0 < port < 65536, "Invalid port.")
        url = f"http://127.0.0.1:{port}"
        with urlopen(url + "/api/health", timeout=1) as response:
            result = json.load(response)
        if result.get("app") == APP_ID and result.get("nonce") == state.get("nonce"):
            return url
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--data-dir", type=Path, default=Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "CodexSessionRepair")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--open", action="store_true")
    args = parser.parse_args()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(args.data_dir / "app.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.setLevel(logging.INFO)
    LOG.addHandler(handler)
    runtime = args.data_dir / "runtime.json"
    with instance_lock(args.data_dir / "instance.lock") as acquired:
        if not acquired:
            for _ in range(20):
                url = existing_url(runtime)
                if url:
                    if args.open:
                        webbrowser.open(url)
                    return
                time.sleep(.25)
            raise RuntimeError("Another instance is starting. Please retry shortly.")
        port_file = args.data_dir / "server-port.json"
        saved_port = args.port
        if not saved_port and port_file.exists():
            saved_port = int(engine.read_json(port_file)["port"])
        try:
            server = LocalServer(args.home, args.data_dir, saved_port)
        except OSError:
            if args.port:
                raise
            server = LocalServer(args.home, args.data_dir, 0)
        engine.save_json(port_file, {"port": server.server_address[1]})
        engine.save_json(runtime, {"port": server.server_address[1], "pid": os.getpid(), "nonce": server.nonce, "home": str(server.service.home)})
        LOG.info("Started %s", server.url)
        if sys.stdout:
            print(server.url, flush=True)
        if args.open:
            webbrowser.open(server.url)
        try:
            server.serve_forever(poll_interval=.2)
        finally:
            server.service.jobs.executor.shutdown(wait=True)
            server.server_close()
            runtime.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        LOG.exception("Application failed")
        raise
