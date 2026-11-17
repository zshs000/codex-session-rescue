import io
import unittest

from relay_transport import ResponseObservation, send_response_body


class Response:
    length = None

    def __init__(self, chunks=(), content_type="text/event-stream"):
        self.chunks = iter([*chunks, b""])
        self.content_type = content_type

    def getheader(self, name, default=""):
        return self.content_type if name == "Content-Type" else default

    def read1(self, size):
        return next(self.chunks)


class TransportTests(unittest.TestCase):
    def test_arbitrary_byte_splits_crlf_unicode_and_multiline_data(self):
        payload = ('event: response.completed\r\n'
                   'data: {"type":"response.completed",\r\n'
                   'data: "response":{"id":"resp_123","output":"中文"}}\r\n\r\n').encode()
        for width in (1, 2, 7, 65536):
            response = Response([payload[i:i + width] for i in range(0, len(payload), width)])
            observation = ResponseObservation(response, True)
            handler = type("Handler", (), {"wfile": io.BytesIO()})()
            send_response_body(handler, response, observation)
            self.assertEqual(handler.wfile.getvalue(), payload)
            self.assertEqual(observation.outcome(), ("completed", ""))
            self.assertEqual(observation.response_id, "resp_123")
            self.assertEqual(observation.bytes_forwarded, len(payload))

    def test_done_and_text_containing_completed_do_not_prove_completion(self):
        response = Response()
        observation = ResponseObservation(response, True)
        observation.feed(b'data: {"type":"response.output_text.delta","delta":"response.completed"}\n\ndata: [DONE]\n\n')
        self.assertEqual(observation.outcome()[0], "missing_completed")

    def test_failure_is_not_erased_by_later_completed(self):
        observation = ResponseObservation(Response())
        observation.feed(b'data: {"type":"response.failed"}\n\ndata: {"type":"response.completed"}\n\n')
        self.assertEqual(observation.outcome()[0], "response.failed")

    def test_event_name_with_invalid_json_does_not_prove_completion(self):
        observation = ResponseObservation(Response())
        observation.feed(b'event: response.completed\ndata: broken-json\n\n')
        self.assertEqual(observation.outcome()[0], "missing_completed")
        self.assertEqual(observation.malformed_events, 1)

    def test_non_stream_json_and_stream_content_type_mismatch(self):
        response = Response(content_type="application/json")
        self.assertEqual(ResponseObservation(response).outcome(), ("http_complete", ""))
        self.assertEqual(ResponseObservation(response, True).outcome()[0], "unexpected_content_type")

    def test_oversized_event_does_not_grow_memory_or_hide_next_event(self):
        observation = ResponseObservation(Response())
        observation.MAX_EVENT = 64
        observation.feed(b'data: ' + b'x' * 100)
        observation.feed(b'\n\ndata: {"type":"response.completed"}\n\n')
        self.assertEqual(observation.outcome()[0], "completed")
        self.assertEqual(observation.oversized_events, 1)

    def test_downstream_write_error_is_distinguished_from_upstream_read(self):
        class Writer:
            def write(self, value):
                raise BrokenPipeError()
        response = Response([b'data: {}\n\n'])
        observation = ResponseObservation(response)
        handler = type("Handler", (), {"wfile": Writer()})()
        with self.assertRaises(BrokenPipeError):
            send_response_body(handler, response, observation)
        self.assertEqual(observation.failure_stage, "downstream_write")
        self.assertEqual(observation.bytes_forwarded, 0)


if __name__ == "__main__":
    unittest.main()
