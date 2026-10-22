import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from app import LocalServer
from fixtures import ROOT_ID, CHILD_ID, GRANDCHILD_ID, OTHER_ID, add_thread, demo_home
import repair_encrypted_session as engine
from session_gateway import SessionGateway, filter_request, request_thread, upstream_url


class FilteringTests(unittest.TestCase):
    def test_codex_thread_headers_and_metadata_agree(self):
        headers = {"thread-id": ROOT_ID, "session-id": ROOT_ID,
                   "x-codex-turn-metadata": json.dumps({"thread_id": ROOT_ID, "session_id": ROOT_ID})}
        body = {"prompt_cache_key": ROOT_ID, "client_metadata": {"thread_id": ROOT_ID}}
        self.assertEqual(request_thread(headers, body), ROOT_ID)
        with self.assertRaisesRegex(engine.RepairError, "冲突"):
            request_thread(headers, {"prompt_cache_key": CHILD_ID})
        self.assertIsNone(request_thread({}, {"input": [{"text": ROOT_ID}]}))

    def test_only_protocol_cipher_is_removed_and_readable_content_is_preserved(self):
        opaque = {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "SECRET"}
        summary = {"type": "reasoning", "id": "rs_2", "summary": [{"type": "summary_text", "text": "Keep this summary"}], "encrypted_content": "SECRET2"}
        tool = {"type": "function_call", "call_id": "tool1", "name": "save", "arguments": '{"encrypted_content":"business data"}'}
        output = {"type": "function_call_output", "call_id": "tool1", "output": '{"encrypted_content":"business result"}'}
        message = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Keep my text"}, {"type": "encrypted_content", "encrypted_content": "SECRET3"}]}
        original = {"model": "test", "input": [opaque, summary, tool, output, message], "tools": [{"parameters": {"properties": {"encrypted_content": {"type": "string"}}}}]}
        filtered, removed = filter_request(original)
        self.assertEqual(removed, 3)
        self.assertEqual(filtered["input"], [{k: v for k, v in summary.items() if k != "encrypted_content"}, tool, output,
                                             {**message, "content": message["content"][:1]}])
        self.assertEqual(filtered["tools"], original["tools"])
        self.assertIn("encrypted_content", original["input"][0])

    def test_opaque_compaction_and_unknown_encryption_are_refused(self):
        for item in ({"type": "compaction", "encrypted_content": "SECRET"},
                     {"type": "new_protocol", "encrypted_content": "SECRET"},
                     {"type": "message", "content": [{"type": "encrypted_content", "encrypted_content": "SECRET"}]}):
            with self.assertRaises(engine.RepairError):
                filter_request({"input": [item]})
        with self.assertRaises(engine.RepairError):
            filter_request({"previous_response_id": "server_history", "input": []})

    def test_urls_accept_base_or_responses_and_refuse_embedded_credentials(self):
        self.assertEqual(upstream_url("https://vendor.example/v1/responses"), "https://vendor.example/v1")
        self.assertEqual(upstream_url("https://vendor.example/v1/responses/compact"), "https://vendor.example/v1")
        for value in ("https://user:secret@vendor.example/v1", "http://vendor.example/v1", "https://vendor.example/v1?key=secret"):
            with self.assertRaises(engine.RepairError):
                upstream_url(value)


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="csr-gateway-")
        self.home = demo_home(Path(self.temp.name) / "home")
        self.state = Path(self.temp.name) / "state"
        self.received = []
        self.finish_stream = threading.Event()
        self.first_chunk = b'event: response.output_text.delta\ndata: {"delta":"hello"}\n\n'
        self.last_chunk = b'event: response.completed\ndata: {"ok":true}\n\n'
        fixture = self

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                fixture.received.append({"path": self.path, "body": raw, "authorization": self.headers.get("Authorization")})
                if self.path.endswith("/responses/compact"):
                    body = b'{"output":[{"type":"compaction","encrypted_content":"new-server-cipher"}]}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    self.wfile.write(fixture.first_chunk)
                    self.wfile.flush()
                    fixture.finish_stream.wait(4)
                    self.wfile.write(fixture.last_chunk)
                    self.wfile.flush()

        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        self.upstream_url = f"http://127.0.0.1:{self.upstream.server_port}/v1"
        self.server = LocalServer(self.home, self.state)
        self.threads = []
        for server in (self.upstream, self.server):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)
        self.addCleanup(self.clean_up)

    def clean_up(self):
        self.finish_stream.set()
        for server, thread in zip((self.upstream, self.server), self.threads):
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
        self.server.service.jobs.executor.shutdown(wait=True)
        self.temp.cleanup()

    def api(self, path, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        headers = {"Content-Type": "application/json", "Origin": self.server.url, "X-Session-Token": self.server.token}
        connection.request("POST" if body is not None else "GET", path, json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        result = (response.status, json.loads(response.read()))
        connection.close()
        return result

    def enable(self, identity=ROOT_ID, include_children=False):
        status, job = self.api("/api/gateway/enable", {"thread_id": identity, "provider_id": "local-fixture",
                                                     "upstream_url": self.upstream_url, "include_children": include_children})
        self.assertEqual(status, 202, job)
        for _ in range(500):
            if job["state"] not in {"queued", "running"}:
                break
            time.sleep(.01)
            _, job = self.api("/api/jobs/" + job["id"])
        self.assertEqual(job["state"], "succeeded", job)
        _, gateway = self.api("/api/gateway/" + identity)
        return gateway

    def proxy(self, gateway, identity, raw, suffix="/responses"):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        path = gateway["proxy_url"].removeprefix(self.server.url) + suffix
        headers = {"Content-Type": "application/json", "Authorization": "Bearer FAKE-TEST-KEY"}
        if identity:
            headers["thread-id"] = identity
        connection.request("POST", path, raw, headers)
        response = connection.getresponse()
        return connection, response

    def test_enable_backs_up_and_cleans_only_selected_thread(self):
        before = {p: p.read_bytes() for p in (self.home / "sessions").glob("*.jsonl")}
        gateway = self.enable()
        self.assertTrue(gateway["enabled"])
        bundle = self.server.service._load_bundle(gateway["backup_id"])
        self.assertEqual([r["id"] for r in bundle["roots"]], [ROOT_ID])
        root = self.server.service.catalog.source(ROOT_ID)
        self.assertEqual((bundle["folder"] / bundle["files"][0]["before_blob"]).read_bytes(), before[root])
        self.assertEqual(engine.encrypted_count([json.loads(r) for r in root.read_bytes().splitlines()]), 0)
        for path, data in before.items():
            if path != root:
                self.assertEqual(path.read_bytes(), data)
        self.assertEqual(self.server.service._verify_wanted(bundle, "after")["index_positions"], True)
        restarted = SessionGateway(self.home, self.state)
        self.assertTrue(restarted.public(ROOT_ID, self.server.url)["enabled"])
        self.assertIn("supports_websockets=false", gateway["resume_command"])

    def test_matching_thread_filters_and_sse_arrives_before_stream_ends(self):
        gateway = self.enable()
        raw = json.dumps({"input": [{"type": "reasoning", "summary": [], "encrypted_content": "SECRET"},
                                   {"type": "message", "role": "user", "content": "Hi"}]}).encode()
        connection, response = self.proxy(gateway, ROOT_ID, raw)
        try:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(len(self.first_chunk)), self.first_chunk)
            self.assertFalse(self.finish_stream.is_set())
            self.finish_stream.set()
            self.assertEqual(response.read(), self.last_chunk)
        finally:
            connection.close()
        upstream = self.received[-1]
        self.assertEqual(upstream["path"], "/v1/responses")
        self.assertNotIn(b"SECRET", upstream["body"])
        self.assertEqual(upstream["authorization"], "Bearer FAKE-TEST-KEY")
        _, status = self.api("/api/gateway/" + ROOT_ID)
        self.assertEqual(status["removed"], 1)
        self.assertTrue(status["connected"])
        saved = (self.state / "gateway-rules.json").read_text()
        self.assertNotIn("FAKE-TEST-KEY", saved)
        self.assertNotIn("SECRET", saved)

    def test_other_threads_missing_id_and_disabled_rule_preserve_exact_body(self):
        gateway = self.enable()
        self.finish_stream.set()
        raw = b'{ "input" : [{"type":"reasoning","encrypted_content":"SECRET"}], "model":"test" }'
        for identity in (CHILD_ID, OTHER_ID, None):
            connection, response = self.proxy(gateway, identity, raw)
            self.assertEqual(response.status, 200)
            response.read()
            connection.close()
            self.assertEqual(self.received[-1]["body"], raw)
        self.api("/api/gateway/disable", {"thread_id": ROOT_ID})
        connection, response = self.proxy(gateway, ROOT_ID, raw)
        self.assertEqual(response.status, 200)
        response.read()
        connection.close()
        self.assertEqual(self.received[-1]["body"], raw)

    def test_family_scope_cleans_existing_and_filters_new_descendants_only(self):
        other_path = self.server.service.catalog.source(OTHER_ID)
        other_before = other_path.read_bytes()
        gateway = self.enable(include_children=True)
        bundle = self.server.service._load_bundle(gateway["backup_id"])
        self.assertEqual({row["id"] for row in bundle["roots"]}, {ROOT_ID, CHILD_ID, GRANDCHILD_ID})
        for identity in (ROOT_ID, CHILD_ID, GRANDCHILD_ID):
            path = self.server.service.catalog.source(identity)
            self.assertEqual(engine.encrypted_count([json.loads(r) for r in path.read_bytes().splitlines()]), 0)
        self.assertEqual(other_path.read_bytes(), other_before)
        self.finish_stream.set()
        raw = b'{"input":[{"type":"reasoning","encrypted_content":"FAMILY-CIPHER"}]}'
        # Load the relationship cache, then create a new nested subagent.
        for identity in (CHILD_ID, GRANDCHILD_ID):
            connection, response = self.proxy(gateway, identity, raw)
            self.assertEqual(response.status, 200)
            response.read(); connection.close()
            self.assertNotIn(b"FAMILY-CIPHER", self.received[-1]["body"])
        new_id = "66666666-6666-4666-8666-666666666666"
        add_thread(self.home, new_id, "new child", parent=GRANDCHILD_ID)
        connection, response = self.proxy(gateway, new_id, raw)
        self.assertEqual(response.status, 200)
        response.read(); connection.close()
        self.assertNotIn(b"FAMILY-CIPHER", self.received[-1]["body"])
        connection, response = self.proxy(gateway, OTHER_ID, raw)
        self.assertEqual(response.status, 200)
        response.read(); connection.close()
        self.assertEqual(self.received[-1]["body"], raw)
        self.api("/api/gateway/disable", {"thread_id": ROOT_ID})
        connection, response = self.proxy(gateway, new_id, raw)
        self.assertEqual(response.status, 200)
        response.read(); connection.close()
        self.assertEqual(self.received[-1]["body"], raw)

    def test_busy_child_prevents_entire_family_enable_without_partial_cleanup(self):
        lock = self.home / "thread-writer-locks" / (CHILD_ID + ".lock")
        lock.parent.mkdir(exist_ok=True)
        lock.write_text("held")
        originals = {p: p.read_bytes() for p in (self.home / "sessions").glob("*.jsonl")}
        _, job = self.api("/api/gateway/enable", {"thread_id": ROOT_ID, "provider_id": "local-fixture",
                                               "upstream_url": self.upstream_url, "include_children": True})
        for _ in range(100):
            if job["state"] == "failed":
                break
            time.sleep(.02)
            _, job = self.api("/api/jobs/" + job["id"])
        self.assertEqual(job["state"], "failed")
        self.assertFalse(self.server.gateway.public(ROOT_ID, self.server.url)["enabled"])
        self.assertEqual({p: p.read_bytes() for p in originals}, originals)

    def test_conflicting_ids_and_opaque_compaction_never_reach_upstream(self):
        gateway = self.enable()
        for body in ({"prompt_cache_key": CHILD_ID, "input": []},
                     {"input": [{"type": "compaction", "encrypted_content": "SECRET"}]}):
            connection, response = self.proxy(gateway, ROOT_ID, json.dumps(body))
            self.assertEqual(response.status, 400)
            response.read()
            connection.close()
        self.assertEqual(self.received, [])

    def test_compact_endpoint_preserves_server_response(self):
        gateway = self.enable()
        connection, response = self.proxy(gateway, ROOT_ID, '{"input":[]}', "/responses/compact")
        self.assertEqual(response.status, 200)
        self.assertIn(b"new-server-cipher", response.read())
        connection.close()
        self.assertEqual(self.received[-1]["path"], "/v1/responses/compact")

    def test_busy_thread_does_not_enable_rule_or_modify_history(self):
        lock = self.home / "thread-writer-locks" / (ROOT_ID + ".lock")
        lock.parent.mkdir(exist_ok=True)
        lock.write_text("held")
        path = self.server.service.catalog.source(ROOT_ID)
        original = path.read_bytes()
        _, job = self.api("/api/gateway/enable", {"thread_id": ROOT_ID, "provider_id": "local-fixture", "upstream_url": self.upstream_url})
        for _ in range(100):
            if job["state"] == "failed":
                break
            time.sleep(.02)
            _, job = self.api("/api/jobs/" + job["id"])
        self.assertEqual(job["state"], "failed")
        self.assertEqual(path.read_bytes(), original)
        self.assertFalse(self.server.gateway.public(ROOT_ID, self.server.url)["enabled"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
