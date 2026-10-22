"""Detach provider replay IDs while preserving local UI IDs and tool pairing."""

import re

OPTIONAL_TOOL_IDS = {"function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output"}
PROVIDER_MESSAGE_ID = re.compile(r"msg_[0-9a-f]{16,}", re.I)


def detach_replay_item(item):
    if not isinstance(item, dict):
        return item, 0
    identity = item.get("id")
    provider_message = (item.get("type") == "message" and isinstance(identity, str) and
                        PROVIDER_MESSAGE_ID.fullmatch(identity))
    if identity and (item.get("type") in OPTIONAL_TOOL_IDS or provider_message):
        return {key: value for key, value in item.items() if key != "id"}, 1
    return item, 0


def normalize_history(value):
    """Visit rollout response items and protocol history arrays, not tool data."""
    if not isinstance(value, dict):
        return value, 0
    result, changed = dict(value), 0
    if value.get("type") == "response_item" and isinstance(value.get("payload"), dict):
        result["payload"], changed = detach_replay_item(value["payload"])
    for key in ("replacement_history", "guardian_history"):
        if isinstance(value.get(key), list):
            children = []
            for item in value[key]:
                detached, count = detach_replay_item(item)
                normalized, nested = normalize_history(detached)
                children.append(normalized)
                changed += count + nested
            result[key] = children
    if value.get("type") in {"compacted", "session_meta"} and isinstance(value.get("payload"), dict):
        result["payload"], nested = normalize_history(value["payload"])
        changed += nested
    return result, changed
