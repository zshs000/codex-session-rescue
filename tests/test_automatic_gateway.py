import copy
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import tomllib
import unittest
from unittest.mock import patch

from app import LocalServer
from provider_config import patch_provider
from automatic_gateway import AutomaticGateway, clean_request
from fixtures import demo_home
import repair_encrypted_session as engine
from relay_transport import open_response, selected_proxy
from urllib.parse import urlsplit


class CleaningTests(unittest.TestCase):
    def test_config_patch_handles_bom_crlf_quoted_provider_and_preserves_other_sections(self):
        raw = b'\xef\xbb\xbfmodel_provider="with.dot"\r\n[model_providers."with.dot"] # keep\r\nbase_url="https://old.example/v1"\r\n[other]\r\nx="untouched"\r\n'
        changed = patch_provider(raw, "with.dot", {"base_url": {"present": True, "value": "http://127.0.0.1:1234/auto/v1"},
                                                  "supports_websockets": {"present": True, "value": False}})
        self.assertTrue(changed.startswith(b'\xef\xbb\xbf'))
        self.assertIn(b'# keep', changed)
        self.assertIn(b'[other]\r\nx="untouched"\r\n', changed)
        config = tomllib.loads(changed.decode('utf-8-sig'))
        self.assertFalse(config["model_providers"]["with.dot"]["supports_websockets"])

    def test_ids_removed_cipher_text_call_pairing_and_business_data_preserved(self):
        body = {"store": False, "input": [
            {"type": "reasoning", "id": "rs_bound", "encrypted_content": "SECRET", "summary": []},
            {"type": "message", "id": "msg_NON_HEX_BOUND", "role": "assistant", "content": "hello", "phase": "commentary"},
            {"type": "function_call", "id": "fc_bound", "call_id": "call_1", "name": "save", "arguments": '{"id":"business"}'},
            {"type": "function_call_output", "id": "fco_bound", "call_id": "call_1", "output": {"id": "business", "encrypted_content": "data"}},
            {"type": "compaction", "id": "cmp_bound", "encrypted_content": "COMPACTION"}],
            "tools": [{"id": "schema-id"}], "metadata": {"id": "meta"}}
        original = copy.deepcopy(body)
        cleaned, counts = clean_request(body)
        self.assertEqual(counts["ids"], 5)
        self.assertEqual(counts["encrypted"], 0)
        self.assertEqual(cleaned["input"][0]["encrypted_content"], "SECRET")
        self.assertEqual(cleaned["input"][1]["phase"], "commentary")
        self.assertEqual(cleaned["input"][2]["call_id"], cleaned["input"][3]["call_id"])
        self.assertEqual(cleaned["input"][3]["output"], body["input"][3]["output"])
        self.assertEqual(cleaned["tools"], body["tools"])
        self.assertEqual(cleaned["input"][4]["encrypted_content"], "COMPACTION")
        self.assertEqual(body, original)

    def test_encrypted_filter_is_independent_and_keeps_readable_summary(self):
        body = {"input": [{"type": "reasoning", "id": "rs_bound", "encrypted_content": "SECRET", "summary": []},
                          {"type": "reasoning", "id": "rs_summary", "encrypted_content": "SECRET2", "summary": [{"text": "keep"}]}]}
        cleaned, counts = clean_request(body, ids=True, encrypted=True)
        self.assertEqual(counts, {"ids": 2, "encrypted": 2, "types": {"reasoning": 2}})
        self.assertEqual(cleaned["input"], [{"type": "reasoning", "summary": [{"text": "keep"}]}])
        preserved, counts = clean_request(body, ids=False, encrypted=False)
        self.assertIs(preserved, body)
        self.assertEqual(counts["ids"], 0)

    def test_server_references_are_never_silently_deleted(self):
        for body in ({"previous_response_id": "resp_old", "input": [{"role": "user", "content": "new only"}]},
                     {"conversation": "conv_old", "input": []},
                     {"input": [{"type": "item_reference", "id": "msg_only_reference"}]}):
            with self.assertRaises(engine.RepairError):
                clean_request(body)
        with self.assertRaises(engine.RepairError):
            clean_request({"input": [{"type": "compaction", "encrypted_content": "STATE"}]}, encrypted=True)


class AutomaticTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="csr-automatic-")
        self.home = demo_home(Path(self.temp.name) / "home")
        self.state = Path(self.temp.name) / "state"
        self.received = []
        self.stream_continue = threading.Event()
        self.first = b'data: {"type":"response.output_text.delta","delta":"hello"}\n\n'
        self.last = b'data: {"type":"response.completed"}\n\n'
        fixture = self

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                fixture.received.append({"body": raw, "auth": self.headers.get("Authorization"), "path": self.path})
                data = json.loads(raw)
                if data.get("test_status"):
                    self.send_response(data["test_status"])
                    self.end_headers()
                    self.wfile.write(b'{"error":"unchanged-upstream-error"}')
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(fixture.first)
                self.wfile.flush()
                fixture.stream_continue.wait(5)
                self.wfile.write(fixture.last)
                self.wfile.flush()

        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        self.url = f"http://127.0.0.1:{self.upstream.server_port}/v1"
        self.config = (f'# keep comment\nmodel_provider = "custom"\nmodel = "keep-model"\n\n'
                       f'[model_providers.custom]\nbase_url = "{self.url}"\nwire_api = "responses"\n'
                       'env_key = "KEEP_KEY_NAME"\n\n[other]\nvalue = "keep-other"\n').encode()
        (self.home / "config.toml").write_bytes(self.config)
        self.server = LocalServer(self.home, self.state)
        self.threads = []
        for server in (self.upstream, self.server):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.stream_continue.set()
        for server, thread in zip((self.upstream, self.server), self.threads):
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
        self.server.service.jobs.executor.shutdown(wait=True)
        self.temp.cleanup()

    def api(self, path, data=None, authorized=True):
        client = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        headers = {"Content-Type": "application/json", "Origin": self.server.url}
        if authorized:
            headers["X-Session-Token"] = self.server.token
        client.request("GET" if data is None else "POST", path, json.dumps(data) if data is not None else None, headers)
        response = client.getresponse()
        result = response.status, json.loads(response.read())
        client.close()
        return result

    def connect(self, **extra):
        status, result = self.api("/api/automatic/connect", {"network": "direct", **extra})
        self.assertEqual(status, 200, result)
        return result

    def relay(self, body, thread=None):
        client = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        headers = {"Content-Type": "application/json", "Authorization": "Bearer FAKE-KEY"}
        if thread:
            headers["thread-id"] = thread
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        client.request("POST", "/auto/v1/responses", raw, headers)
        return client, client.getresponse()

    def wait_idle(self):
        for _ in range(200):
            if self.server.automatic.active == 0:
                return
            time.sleep(.01)
        self.fail("relay never became idle")

    def test_one_click_configures_all_threads_without_touching_history(self):
        originals = {p: p.read_bytes() for p in self.home.rglob("*.jsonl")}
        result = self.connect()
        self.assertTrue(result["attached"])
        config = tomllib.loads((self.home / "config.toml").read_text())
        self.assertEqual(config["model_providers"]["custom"]["base_url"], self.server.url + "/auto/v1")
        self.assertFalse(config["model_providers"]["custom"]["supports_websockets"])
        self.assertEqual(config["model_providers"]["custom"]["env_key"], "KEEP_KEY_NAME")
        self.assertEqual(config["other"]["value"], "keep-other")
        self.assertEqual({p: p.read_bytes() for p in originals}, originals)
        self.assertTrue(list((self.state / "config-backups").glob("*.toml")))

    def test_ids_only_sse_auth_and_count_survive_restart_and_reset(self):
        self.connect()
        body = {"input": [{"type": "reasoning", "id": "rs_BOUND", "summary": [], "encrypted_content": "SECRET"},
                          {"type": "message", "id": "msg_BOUND", "role": "user", "content": "hello"}]}
        client, response = self.relay(body)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(len(self.first)), self.first)
        self.assertFalse(self.stream_continue.is_set())
        forwarded = json.loads(self.received[-1]["body"])
        self.assertEqual(forwarded["input"][0]["encrypted_content"], "SECRET")
        self.assertNotIn("id", forwarded["input"][0])
        self.assertEqual(self.received[-1]["auth"], "Bearer FAKE-KEY")
        self.stream_continue.set()
        self.assertEqual(response.read(), self.last)
        client.close(); self.wait_idle()
        restarted = AutomaticGateway(self.home, self.state)
        self.assertEqual(restarted.public(self.server.url)["counters"]["ids"], 2)
        saved = (self.state / "automatic-gateway.json").read_text()
        self.assertNotIn("FAKE-KEY", saved)
        self.assertNotIn("SECRET", saved)
        status, zero = self.api("/api/automatic/reset", {})
        self.assertEqual(status, 200)
        self.assertEqual(zero["counters"]["ids"], 0)
        self.assertTrue(zero["attached"])
        client, response = self.relay(body, thread="new-child-anywhere")
        response.read(); client.close(); self.wait_idle()
        self.assertEqual(self.server.automatic.counters["ids"], 2)

    def test_reset_during_stream_does_not_readd_old_request(self):
        self.connect()
        client, response = self.relay({"input": [{"type": "message", "id": "msg_bound", "content": "hi"}]})
        response.read(len(self.first))
        self.api("/api/automatic/reset", {})
        self.stream_continue.set()
        response.read(); client.close(); self.wait_idle()
        self.assertEqual(self.server.automatic.counters["ids"], 0)
        self.assertEqual(self.server.automatic.counters["requests"], 0)
        self.assertEqual(list(self.server.automatic.events), [])

    def test_encrypted_mode_and_upstream_errors_are_preserved(self):
        self.connect(filter_encrypted=True)
        self.stream_continue.set()
        client, response = self.relay({"input": [{"type": "reasoning", "id": "rs_bound", "summary": [], "encrypted_content": "SECRET"}]})
        response.read(); client.close(); self.wait_idle()
        self.assertEqual(json.loads(self.received[-1]["body"])["input"], [])
        self.assertEqual(self.server.automatic.counters["encrypted"], 1)
        for status in (400, 429, 503):
            client, response = self.relay({"input": [], "test_status": status})
            self.assertEqual(response.status, status)
            self.assertEqual(response.read(), b'{"error":"unchanged-upstream-error"}')
            client.close(); self.wait_idle()
        self.assertEqual(len(self.received), 4)
        self.assertEqual(self.server.automatic.counters["errors"], 3)

    def test_refs_are_rejected_and_mutations_need_authorization(self):
        self.connect()
        client, response = self.relay({"previous_response_id": "resp_REF", "input": []})
        self.assertEqual(response.status, 400)
        response.read(); client.close(); self.wait_idle()
        self.assertFalse(self.received)
        status, _ = self.api("/api/automatic/reset", {}, authorized=False)
        self.assertEqual(status, 403)

    def test_disable_restores_owned_fields_and_keeps_other_edits(self):
        self.connect()
        path = self.home / "config.toml"
        path.write_bytes(path.read_bytes().replace(b'keep-model', b'new-model'))
        status, result = self.api("/api/automatic/disconnect", {})
        self.assertEqual(status, 200)
        restored = tomllib.loads(path.read_text())
        self.assertEqual(restored["model_providers"]["custom"]["base_url"], self.url)
        self.assertNotIn("supports_websockets", restored["model_providers"]["custom"])
        self.assertEqual(restored["model"], "new-model")
        self.assertFalse(result["enabled"])

    def test_port_migration_and_external_config_change(self):
        self.connect()
        gateway = self.server.automatic
        gateway.reconnect("http://127.0.0.1:19999")
        config = tomllib.loads((self.home / "config.toml").read_text())
        self.assertEqual(config["model_providers"]["custom"]["base_url"], "http://127.0.0.1:19999/auto/v1")
        path = self.home / "config.toml"
        path.write_text(path.read_text().replace("http://127.0.0.1:19999/auto/v1", "https://new-vendor.example/v1"))
        before = path.read_bytes()
        gateway.reconnect(self.server.url)
        self.assertEqual(path.read_bytes(), before)
        gateway.disconnect(self.server.url)
        self.assertEqual(tomllib.loads(path.read_text())["model_providers"]["custom"]["base_url"], "https://new-vendor.example/v1")

    def test_failed_config_write_restores_settings(self):
        with patch("automatic_gateway.write_config", side_effect=engine.RepairError("simulated config failure")):
            status, _ = self.api("/api/automatic/connect", {"network": "direct"})
        self.assertEqual(status, 400)
        self.assertFalse(self.server.automatic.settings["enabled"])
        self.assertEqual((self.home / "config.toml").read_bytes(), self.config)


class NetworkTests(unittest.TestCase):
    def test_auto_reads_current_proxy_each_time_without_clash_api(self):
        with patch("relay_transport.proxy_bypass", return_value=False), patch("relay_transport.getproxies", return_value={"https": "http://127.0.0.1:7897"}):
            self.assertEqual(selected_proxy(urlsplit("https://vendor.example/v1"), "auto"), "http://127.0.0.1:7897")
        with patch("relay_transport.proxy_bypass", return_value=False), patch("relay_transport.getproxies", return_value={"https": "http://127.0.0.1:19876"}):
            self.assertEqual(selected_proxy(urlsplit("https://vendor.example/v1"), "auto"), "http://127.0.0.1:19876")
        self.assertIsNone(selected_proxy(urlsplit("https://vendor.example/v1"), "direct"))

    def test_https_proxy_uses_connect_and_keeps_target_tls_host(self):
        with patch("relay_transport.http.client.HTTPSConnection") as factory:
            open_response("POST", "https://vendor.example/v1/responses", {"Authorization": "Bearer TEST"}, b'{}', "http://127.0.0.1:7897")
            factory.assert_called_once_with("127.0.0.1", 7897, timeout=300)
            factory.return_value.set_tunnel.assert_called_once_with("vendor.example", 443)
            factory.return_value.request.assert_called_once_with("POST", "/v1/responses", b'{}', {"Authorization": "Bearer TEST"})


if __name__ == "__main__":
    unittest.main()
