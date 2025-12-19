import logging

import pytest


class _FakeResponsesResponse:
    """Simulates OpenAI Responses API response object"""
    def __init__(self, *, output_text=None, output=None, finish_reason="stop", response_id="resp_123", usage=None):
        self.id = response_id
        self.model = "fake-model"
        self.created = 0
        self.finish_reason = finish_reason
        self.usage = usage or {"prompt_tokens": 1, "completion_tokens": 0, "total_tokens": 1, "reasoning_tokens": 0}
        self.output_text = output_text or ""
        self.output = output or []


class _FakeResponses:
    def __init__(self, response: _FakeResponsesResponse):
        self._response = response

    def create(self, *args, **kwargs):
        return self._response


class _FakeOpenAIClient:
    def __init__(self, response: _FakeResponsesResponse):
        self.responses = _FakeResponses(response)


def _call_unwrapped(method, *args, **kwargs):
    """
    Call a tenacity-wrapped method without retries/backoff.
    Tenacity's @retry decorator preserves __wrapped__.
    """
    unwrapped = getattr(method, "__wrapped__", None)
    assert unwrapped is not None, "Expected tenacity-wrapped function to have __wrapped__"
    return unwrapped(*args, **kwargs)


def test_span_detector_logs_on_empty_message_content(caplog):
    from src.redaction.llm_span_detector import LLMSpanDetector

    detector = LLMSpanDetector(
        api_key="test-key",
        model="gpt-5-mini",
        client=_FakeOpenAIClient(
            _FakeResponsesResponse(
                output_text="",
                output=[],
                finish_reason="stop",
                response_id="resp_empty_content",
            )
        ),
    )

    caplog.set_level(logging.ERROR)
    window_text = "Hello John Smith at Morgan Stanley"

    with pytest.raises(RuntimeError) as e:
        _call_unwrapped(detector._detect_spans_in_window, detector, window_text, 0)

    assert "Empty response content from LLM span detection" in str(e.value)

    logs = "\n".join(r.message for r in caplog.records)
    assert "llm_span_detection_empty_content" in logs
    assert "response_id=resp_empty_content" in logs
    assert "finish_reason=stop" in logs
    # Ensure we never log raw document text
    assert window_text not in logs


def test_span_detector_logs_on_json_decode_error_without_leaking_content(caplog):
    from src.redaction.llm_span_detector import LLMSpanDetector

    detector = LLMSpanDetector(
        api_key="test-key",
        model="gpt-5-mini",
        client=_FakeOpenAIClient(
            _FakeResponsesResponse(
                output_text="not-json",
                output=[],
                finish_reason="stop",
                response_id="resp_bad_json",
            )
        ),
    )

    caplog.set_level(logging.ERROR)
    window_text = "This is a short string; the content is controlled by the fake response."

    spans = _call_unwrapped(detector._detect_spans_in_window, detector, window_text, 0)
    assert spans == []

    logs = "\n".join(r.message for r in caplog.records)
    assert "llm_span_detection_json_decode_error" in logs
    assert "response_id=resp_bad_json" in logs
    assert "content_len=8" in logs
    assert "content_sha256_16=" in logs
    # Ensure we never log the raw response content
    assert "not-json" not in logs


