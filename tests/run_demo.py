"""Serve synthetic demo data. Does not read or modify the user's Codex home."""
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app import LocalServer
from fixtures import demo_home

artifacts = ROOT / "test-artifacts"
artifacts.mkdir(exist_ok=True)
directory = Path(tempfile.mkdtemp(prefix="demo-", dir=artifacts))
home = demo_home(directory / "home")
(home / ".synthetic-fixture").write_text("Synthetic test data only", encoding="utf-8")
server = LocalServer(home, directory / "state")
runtime = {"url": server.url, "home": str(home), "synthetic": True, "nonce": server.nonce}
(artifacts / "demo-runtime.json").write_text(json.dumps(runtime), encoding="utf-8")
print(json.dumps(runtime), flush=True)
try:
    server.serve_forever(poll_interval=.2)
finally:
    server.service.jobs.executor.shutdown(wait=True)
    server.server_close()
