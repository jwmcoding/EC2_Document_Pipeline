# Redaction Troubleshooting Notes

## LLM span detection: “Empty response content” failures

### Symptom
- Redaction runs intermittently fail in the LLM span stage with errors like:
  - `Empty response content from LLM span detection`
  - followed by tenacity `RetryError[...]` after retries
- In logs, you may still see:
  - `HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"`
- Net effect: span detection returns no spans, and strict pipelines may mark the document as failed.

### What we observed in the FMV `.docx` harness run
- The OpenAI request returned **HTTP 200**, but `response.choices[0].message.content` was **empty** (`None` or `""`).
- This is **not** a JSON parsing problem (we never got JSON text to parse).
- This is **not** a DOCX extraction problem (text extraction succeeded; the failure happened during the LLM call).
- Similar long-running runs can also show intermittent **connection errors**, suggesting some network/service instability.

### Most likely causes (ranked)
- **Response shape where `message.content` is empty**:
  - The model may return a non-text payload (e.g., a refusal field, tool-call style response, or other structured response) leaving `message.content` empty.
  - The Python SDK can surface these as HTTP 200 with empty `content`.
- **Transient upstream service issue**:
  - Rare but real: a 200 OK response that carries no usable content.
  - Retries sometimes succeed later in the same run, consistent with intermittent service hiccups.
- **Connection instability / interrupted response**:
  - Particularly during long runs; partial/failed response handling can manifest as empty content depending on transport/SDK behavior.
- **`response_format=json_schema` edge case**:
  - If the model can’t comply, it should refuse/error, but occasionally may surface as empty content.

### How to confirm the exact cause next time (recommended logging)
When `message.content` is empty, log **additional response fields** so we can distinguish refusal vs tool-calls vs true empty payload:
- `response.choices[0].finish_reason`
- `response.choices[0].message.refusal` (if present in this SDK/model)
- `response.choices[0].message.tool_calls` (if present)
- Any request/response IDs available from the client/SDK (for correlation)

### Logging added (Dec 2025): what we now emit
Implemented in `src/redaction/llm_span_detector.py` at the two failure points (single-window and batch-window calls).

When the SDK returns HTTP 200 but `response.choices[0].message.content` is empty, we now log one of:
- `llm_span_detection_empty_content`
- `llm_span_detection_empty_content_batch`

When JSON parsing fails (non-JSON or truncated JSON in `message.content`), we now log:
- `llm_span_detection_json_decode_error`
- `llm_span_detection_json_decode_error_batch`

**Fields logged (high-signal, no document text):**
- `response_id`, `response_model`, `created`
- `finish_reason`
- `has_refusal` (boolean), `tool_calls_present` (boolean)
- `usage` (token counts if present)
- window sizing context (`window_chars`, `global_offset`) or batching context (`windows_count`, `total_window_chars`)
- for JSON decode errors: `content_len` and `content_sha256_16` (hash prefix to correlate failures without logging content)

### Practical notes
- This shows up more often in longer documents / longer runs because:
  - longer latency, more load, more opportunities for transient failures
  - more total requests per session

### Decision point for “strict mode”
Clarify semantics:
- **Option A**: strict mode means **fail only if validators fail** (LLM span failures become warnings, deterministic redaction still runs).
- **Option B**: strict mode means **fail if the LLM span stage fails** (current behavior in some flows).

For the harness and production redaction, Option A is often more robust if LLM spans are considered an enhancement layer.


