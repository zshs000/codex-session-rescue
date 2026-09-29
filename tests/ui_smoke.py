"""Exercise the UI only against explicitly marked synthetic demo data."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import threading
from urllib.request import urlopen

from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "test-artifacts"
sys.path.insert(0, str(ROOT))
from app import LocalServer
from fixtures import demo_home
ARTIFACTS.mkdir(exist_ok=True)
directory = Path(tempfile.mkdtemp(prefix="ui-", dir=ARTIFACTS))
fixture_home = demo_home(directory / "home")
(fixture_home / ".synthetic-fixture").write_text("Synthetic test data only", encoding="utf-8")
server = LocalServer(fixture_home, directory / "state")
server_thread = threading.Thread(target=server.serve_forever, daemon=True)
server_thread.start()
runtime = {"url": server.url, "home": str(fixture_home), "synthetic": True, "nonce": server.nonce}
(ARTIFACTS / "demo-runtime.json").write_text(json.dumps(runtime), encoding="utf-8")
home = Path(runtime["home"]).resolve()
assert runtime["synthetic"] and home.is_relative_to(ARTIFACTS.resolve()) and (home / ".synthetic-fixture").is_file()
assert runtime["url"].startswith("http://127.0.0.1:")
with urlopen(runtime["url"] + "/api/bootstrap") as response:
    info = json.load(response)
assert Path(info["home"]).resolve() == home, "Never run mutation UI checks against real Codex data."
originals = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (home / "sessions").glob("*.jsonl")}
browser_path = Path.home() / "AppData/Local/ms-playwright/chromium-1223/chrome-win64/chrome.exe"
ROOT_ID = "11111111-1111-4111-8111-111111111111"
SPLIT_ID = "44444444-4444-4444-8444-444444444444"
errors = []

with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=str(browser_path), headless=True)
    context = browser.new_context(viewport={"width": 1440, "height": 1100}, reduced_motion="reduce", accept_downloads=True)
    page = context.new_page()
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
    page.set_default_timeout(15000)
    try:
        page.goto(runtime["url"], wait_until="networkidle")
        expect(page.get_by_role("heading", name="选择一条需要修复的会话")).to_be_visible()
        page.screenshot(path=str(ARTIFACTS / "ui-welcome.png"), full_page=True)
        page.locator("#searchInput").fill("codex resume " + ROOT_ID)
        expect(page.locator(".session-card")).to_have_count(1)
        page.locator(".session-card").click()
        expect(page.locator("#inspectButton")).to_be_visible()
        page.locator("#inspectButton").click()
        expect(page.locator("#cleanButton")).to_be_enabled()
        expect(page.locator(".tree-row")).to_have_count(3)
        expect(page.locator(".tree-row input:checked")).to_have_count(2)
        page.screenshot(path=str(ARTIFACTS / "ui-inspection.png"), full_page=True)
        page.locator("#cleanButton").click()
        expect(page.locator("#notice")).to_contain_text("清理完成")
        expect(page.locator("#jobPanel")).to_be_hidden()
        page.locator("#backupsTab").click()
        expect(page.locator(".backup-card")).to_have_count(1)
        page.screenshot(path=str(ARTIFACTS / "ui-backups.png"), full_page=True)
        page.get_by_role("button", name="回滚到清理前", exact=True).click()
        expect(page.locator("#notice")).to_contain_text("已恢复清理前")
        expect(page.locator("#jobPanel")).to_be_hidden()
        with page.expect_download() as download_event:
            page.get_by_role("button", name="导出备份", exact=True).click()
        download = download_event.value
        download.save_as(str(ARTIFACTS / "ui-export-test.zip"))
        assert (ARTIFACTS / "ui-export-test.zip").stat().st_size > 0
        page.locator("#sessionsTab").click()
        page.locator("#searchInput").fill("codex resume " + SPLIT_ID)
        expect(page.locator(".session-card")).to_have_count(1)
        expect(page.locator(".session-card")).to_contain_text("多轮回滚")
        page.locator(".session-card").click()
        page.locator("#inspectButton").click()
        expect(page.locator("#cleanButton")).to_be_enabled()
        expect(page.locator(".segment-card")).to_have_count(3)
        page.screenshot(path=str(ARTIFACTS / "ui-split-history.png"), full_page=True)
        page.locator("#cleanButton").click()
        expect(page.locator("#notice")).to_contain_text("清理完成")
        expect(page.locator("#jobPanel")).to_be_hidden()
        page.locator("#backupsTab").click()
        card = page.locator(".backup-card").filter(has_text="多轮回滚后的研究会话")
        expect(card).to_have_count(1)
        card.get_by_role("button", name="回滚到清理前", exact=True).click()
        expect(page.locator("#notice")).to_contain_text("已恢复清理前")
        expect(page.locator("#jobPanel")).to_be_hidden()
        page.locator("#sessionsTab").click()
        page.locator("#inspectButton").click()
        expect(page.locator("#cleanButton")).to_be_enabled()
        page.set_viewport_size({"width": 390, "height": 844})
        page.screenshot(path=str(ARTIFACTS / "ui-mobile.png"), full_page=True)
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1"), "Horizontal overflow at narrow width"
        assert not errors, errors
        current = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (home / "sessions").glob("*.jsonl")}
        assert current == originals, "UI rollback must restore every original synthetic file."
        report = {"ok": True, "search_resume_command": True, "recursive_subagents": True,
                  "parent_and_child_cleanup_and_rollback": True, "split_cleanup_and_rollback": True,
                  "backup_export": True, "source_files_restored_exactly": True, "narrow_viewport_no_overflow": True,
                  "browser_errors": errors, "synthetic_home": str(home)}
        (ARTIFACTS / "ui-verification.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False), flush=True)
    except BaseException:
        page.screenshot(path=str(ARTIFACTS / "ui-failure.png"), full_page=True)
        raise
    finally:
        browser.close()
        server.shutdown()
        server_thread.join(timeout=3)
        server.service.jobs.executor.shutdown(wait=True)
        server.server_close()
