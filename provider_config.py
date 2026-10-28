"""Surgical, reversible edits to the selected provider; no credentials are read out."""
import json
from pathlib import Path
import re
import tomllib
import uuid

import repair_encrypted_session as engine


def read_config(home, require_provider=True):
    path = Path(home) / "config.toml"
    raw = path.read_bytes()
    config = tomllib.loads(raw.decode("utf-8-sig"))
    provider = config.get("model_provider")
    if require_provider:
        engine.require(isinstance(provider, str) and provider in config.get("model_providers", {}),
                       "当前厂商没有可编辑的配置段，请先在 Codex 中选好厂商。")
    return raw, config, provider


def patch_provider(raw, provider, changes):
    text = raw.decode("utf-8-sig")
    headers = list(re.finditer(r"(?m)^[ \t]*\[[^\r\n]+\][ \t]*(?:#[^\r\n]*)?\r?$", text))
    for index, match in enumerate(headers):
        try:
            parsed = tomllib.loads(match.group().rstrip("\r"))
        except tomllib.TOMLDecodeError:
            continue
        if parsed != {"model_providers": {provider: {}}}:
            continue
        end = headers[index + 1].start() if index + 1 < len(headers) else len(text)
        block = text[match.end():end]
        newline = "\r\n" if "\r\n" in text else "\n"
        for key, value in changes.items():
            engine.require(key in {"base_url", "supports_websockets"}, "不支持修改这个配置项。")
            pattern = re.compile(r"(?m)^[ \t]*" + re.escape(key) + r"[ \t]*=[^\r\n]*(?:\r?\n|$)")
            found = list(pattern.finditer(block))
            engine.require(len(found) <= 1, "厂商配置项重复，请先修正配置。")
            replacement = "" if not value["present"] else key + " = " + json.dumps(value["value"], ensure_ascii=False) + newline
            if found:
                item = found[0]
                block = block[:item.start()] + replacement + block[item.end():]
            elif replacement:
                block = block.rstrip("\r\n") + newline + replacement + newline
        changed = text[:match.end()] + block + text[end:]
        encoded = changed.encode("utf-8")
        if raw.startswith(b"\xef\xbb\xbf"):
            encoded = b"\xef\xbb\xbf" + encoded
        tomllib.loads(encoded.decode("utf-8-sig"))
        return encoded
    raise engine.RepairError("没有找到独立的厂商配置段，未修改配置。")


def write_config(home, state_dir, before, after):
    if before == after:
        return
    path = Path(home) / "config.toml"
    backup_dir = Path(state_dir) / "config-backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    (backup_dir / ("config-" + uuid.uuid4().hex + ".toml")).write_bytes(before)
    engine.require(path.read_bytes() == before, "配置在接入期间发生变化，请刷新后重试。")
    stage = path.with_name("config.gateway-" + uuid.uuid4().hex + ".tmp")
    try:
        stage.write_bytes(after)
        engine.require(path.read_bytes() == before, "配置在接入期间发生变化，请刷新后重试。")
        stage.replace(path)
    finally:
        stage.unlink(missing_ok=True)


def snapshot_fields(provider):
    return {key: {"present": key in provider, "value": provider.get(key)}
            for key in ("base_url", "supports_websockets")}
