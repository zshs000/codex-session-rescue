import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest

from app import LocalServer
from fixtures import ROOT_ID, demo_home


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="csr-http-")
        self.home = demo_home(Path(self.temp.name) / "home")
        self.server = LocalServer(self.home, Path(self.temp.name) / "state")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        self.server.shutdown()
        self.thread.join(timeout=3)
        self.server.service.jobs.executor.shutdown(wait=True)
        self.server.server_close()
        assert Path(self.temp.name).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve())
        self.temp.cleanup()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=5)
        try:
            connection.request(method, path, json.dumps(body) if body is not None else None, headers or {})
            response = connection.getresponse()
            return response.status, response.read(), dict(response.getheaders())
        finally:
            connection.close()

    def test_index_has_local_content_security_policy(self):
        status, body, headers = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("frame-ancestors 'none'", headers["Content-Security-Policy"])
        self.assertNotIn("unsafe-inline", headers["Content-Security-Policy"])
        self.assertIn(b"/app.js", body)

    def test_missing_token_foreign_origin_and_rebound_host_are_rejected(self):
        body = {"thread_id": ROOT_ID}
        status, _, _ = self.request("POST", "/api/inspect", body, {"Content-Type": "application/json", "Origin": self.server.url})
        self.assertEqual(status, 403)
        status, _, _ = self.request("POST", "/api/inspect", body, {"Content-Type": "application/json", "Origin": "https://untrusted.invalid", "X-Session-Token": self.server.token})
        self.assertEqual(status, 403)
        status, _, _ = self.request("GET", "/api/bootstrap", headers={"Host": "untrusted.invalid"})
        self.assertEqual(status, 403)

    def test_authenticated_inspection_and_search(self):
        status, body, _ = self.request("GET", "/api/sessions?q=" + ROOT_ID)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["items"][0]["id"], ROOT_ID)
        status, body, _ = self.request("POST", "/api/inspect", {"thread_id": ROOT_ID}, {
            "Content-Type": "application/json", "Origin": self.server.url, "X-Session-Token": self.server.token})
        self.assertEqual(status, 202)
        self.assertIn("id", json.loads(body))
        self.assertNotIn(b"TEST-SECRET-CIPHER", body)

    def test_no_directory_or_backup_path_traversal(self):
        for path in ("/../repair_service.py", "/api/backups/../../auth.json/export"):
            status, _, _ = self.request("GET", path, headers={"X-Session-Token": self.server.token})
            self.assertIn(status, (400, 404))


if __name__ == "__main__":
    unittest.main(verbosity=2)
