"""Agent task payloads must survive reasoning-history filtering."""
import copy
import unittest

from automatic_gateway import clean_request
from session_gateway import filter_request


class AgentPayloadTests(unittest.TestCase):
    def test_agent_payload_survives_both_gateway_routes(self):
        cipher = {"type": "encrypted_content", "encrypted_content": "TEST-AGENT-PAYLOAD"}
        prefix = {"type": "input_text", "text": "Message Type: MESSAGE\nPayload:\n"}
        for content in ([prefix, cipher], [cipher]):
            for route in ("automatic", "session"):
                with self.subTest(content=content, route=route):
                    message = {"type": "message", "role": "user", "content": content}
                    body = {"input": [
                        {"type": "reasoning", "summary": [], "encrypted_content": "TEST-REASONING"},
                        message,
                    ]}
                    original = copy.deepcopy(body)
                    if route == "automatic":
                        cleaned, counts = clean_request(body, ids=True, encrypted=True)
                        removed = counts["encrypted"]
                    else:
                        cleaned, removed = filter_request(body)
                    self.assertEqual(cleaned["input"], [message])
                    self.assertEqual(removed, 1)
                    self.assertEqual(body, original)

    def test_standalone_encrypted_payload_is_not_assumed_to_be_reasoning(self):
        body = {"input": [{"type": "encrypted_content", "encrypted_content": "TEST-PAYLOAD"}]}
        cleaned, counts = clean_request(body, ids=True, encrypted=True)
        self.assertEqual(cleaned, body)
        self.assertEqual(counts["encrypted"], 0)


if __name__ == "__main__":
    unittest.main()
