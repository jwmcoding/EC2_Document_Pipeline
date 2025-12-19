# Plan: Revert to Always Using OCR + TableFormer on Every PDF

**Date**: December 13, 2025  
**Goal**: Revert auto OCR changes and always use OCR + TableFormer ACCURATE on every PDF  
**Reason**: After examining results, quality-first approach is preferred over performance optimization

---

## 📋 Current State Analysis

### What Was Changed (Auto OCR Implementation)
1. **Two-pass system**:
   - Pass 1: OCR OFF, TableFormer ACCURATE ON (with quality checks)
   - Pass 2: OCR ON, TableFormer ACCURATE ON (fallback if Pass 1 fails)
   
2. **Quality metrics**:
   - `_compute_quality_metrics()` - calculates text_chars, word_count, alnum_ratio, table_count
   - `_check_pass1_quality()` - evaluates if Pass 1 is sufficient
   
3. **OCR mode parameter**:
   - `ocr_mode`: "auto" (default), "on", "off"
   - CLI argument `--docling-ocr-mode` with default "auto"
   
4. **Timeout splitting**:
   - Pass 1 timeout: 20 seconds (15% of total, max 20s)
   - Pass 2 timeout: remainder (220 seconds if total is 240s)

5. **Metadata recording**:
   - `docling_ocr_mode`, `docling_ocr_used`, `docling_pass_used`
   - Quality metrics stored in processing_status

### What Should Remain
- ✅ TableFormer ACCURATE mode (already always enabled)
- ✅ Metadata recording (docling_metadata in processing_status)
- ✅ Timeout handling (single timeout, no splitting)
- ✅ CLI arguments (but default to "on" instead of "auto")
- ✅ Text truncation for Pinecone metadata (37KB limit)

---

## 🎯 Reversion Plan

### Phase 1: Simplify DoclingParser Core Logic

**File**: `src/parsers/docling_parser.py`

**Changes**:
1. **Simplify `_parse_pdf_content()`**:
   - Remove two-pass logic
   - Remove quality checks
   - Always use OCR-enabled converter
   - Single pass with full timeout

2. **Update `__init__()`**:
   - Change default `ocr_mode` from `"auto"` to `"on"`
   - Keep backward compatibility for `ocr` bool parameter
   - Remove quality threshold parameters (or keep but don't use)

3. **Simplify `_create_converter()`**:
   - Always create OCR-enabled converter (no conditional logic)
   - Keep TableFormer ACCURATE mode (already correct)

4. **Keep helper functions** (for potential future use):
   - `_compute_quality_metrics()` - keep but mark as unused
   - `_check_pass1_quality()` - keep but mark as unused

### Phase 2: Update Default Behavior

**Files**:
- `process_discovered_documents.py` - Change CLI default
- `src/pipeline/document_processor.py` - Change default when creating parser
- `src/pipeline/parallel_processor.py` - Change default in worker initialization

**Changes**:
1. **CLI default**: Change `--docling-ocr-mode` default from `"auto"` to `"on"`
2. **DocumentProcessor**: Pass `ocr_mode="on"` when creating DoclingParser
3. **ParallelProcessor**: Pass `ocr_mode="on"` in worker config

### Phase 3: Clean Up Metadata

**Files**:
- `src/parsers/docling_parser.py` - Simplify metadata recording
- `process_discovered_documents.py` - Update docling metadata extraction

**Changes**:
1. **Metadata recording**:
   - Always record `docling_ocr_mode: "on"`
   - Always record `docling_ocr_used: True`
   - Remove `docling_pass_used` (no longer relevant)
   - Keep quality metrics if computed, but mark as informational only

### Phase 4: Update Documentation

**Files**:
- `memory-bank/activeContext.md` - Update current state
- `process_discovered_documents.py` - Update help text

**Changes**:
1. Document that OCR is always enabled
2. Update help text for `--docling-ocr-mode` to reflect new default
3. Note that "auto" mode is deprecated/removed

---

## 🔧 Detailed Implementation Steps

### Step 1: Simplify `DoclingParser._parse_pdf_content()`

**Current**: Two-pass with quality checks  
**Target**: Single pass with OCR always enabled

```python
def _parse_pdf_content(self, content: bytes, metadata: Dict[str, Any]) -> ParsedContent:
    """
    Parse PDF content using Docling with OCR and TableFormer ACCURATE always enabled.
    """
    # Always use OCR-enabled converter
    if self.ocr_mode == "off":
        # Only exception: explicit "off" mode
        return self._parse_single_pass(content, metadata, use_ocr=False, pass_num=1)
    
    # Default: Always use OCR
    return self._parse_single_pass(
        content, 
        metadata, 
        use_ocr=True, 
        pass_num=1, 
        timeout_seconds=self.timeout_seconds
    )
```

### Step 2: Update `DoclingParser.__init__()`

**Change default**:
```python
def __init__(
    self,
    ocr_mode: Optional[Literal["auto", "on", "off"]] = None,
    ocr: Optional[bool] = None,  # Backward compatibility
    timeout_seconds: int = 240,
    # Remove or deprecate quality threshold parameters
    min_text_chars: int = 800,  # Keep for backward compat but unused
    min_word_count: int = 150,  # Keep for backward compat but unused
    alnum_threshold: float = 0.5,  # Keep for backward compat but unused
) -> None:
    # Default to "on" instead of "auto"
    if ocr_mode is None:
        ocr_mode = "on"  # Changed from "auto"
    
    # ... rest of initialization
```

### Step 3: Simplify `_create_converter()`

**Current**: Conditional OCR based on `ocr_enabled` parameter  
**Target**: Always create OCR-enabled converter (unless explicitly "off")

```python
def _create_converter(self, ocr_enabled: bool) -> Any:
    """Create a Docling converter with OCR and TableFormer ACCURATE always enabled."""
    try:
        pipeline_options = PdfPipelineOptions()
        
        # Always enable TableFormer ACCURATE for table fidelity
        pipeline_options.do_table_structure = True
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
        pipeline_options.table_structure_options.do_cell_matching = True
        
        # Always enable OCR (unless explicitly disabled)
        if ocr_enabled:
            pipeline_options.do_ocr = True
            pipeline_options.images_scale = 2.0
            # ... OCR configuration (EasyOCR/Tesseract)
        
        return DoclingDocumentConverter(
            format_options={PdfFormatOption: pipeline_options}
        )
    except Exception as e:
        # ... error handling
```

### Step 4: Update CLI Defaults

**File**: `process_discovered_documents.py`

```python
parser.add_argument(
    "--docling-ocr-mode",
    type=str,
    choices=["auto", "on", "off"],
    default="on",  # Changed from "auto"
    help=(
        "Docling OCR behavior mode (default: on). "
        "'on' always uses OCR with TableFormer ACCURATE. "
        "'off' disables OCR. "
        "'auto' is deprecated and treated as 'on'."
    ),
)
```

### Step 5: Update DocumentProcessor

**File**: `src/pipeline/document_processor.py`

```python
# When creating DoclingParser
docling_kwargs = docling_kwargs or {}
docling_kwargs.setdefault("ocr_mode", "on")  # Changed from "auto"

self.parser = DoclingParser(
    ocr_mode="on",  # Explicit default
    timeout_seconds=240,
    **docling_kwargs
)
```

### Step 6: Update ParallelProcessor

**File**: `src/pipeline/parallel_processor.py`

```python
# In worker_initializer
parser = DoclingParser(
    ocr_mode=config.get("docling_ocr_mode", "on"),  # Changed from "auto"
    timeout_seconds=config.get("docling_timeout_seconds", 240),
    # Quality thresholds kept for backward compat but unused
    min_text_chars=config.get("docling_min_text_chars", 800),
    min_word_count=config.get("docling_min_word_count", 150),
    alnum_threshold=config.get("docling_alnum_threshold", 0.5),
)
```

### Step 7: Simplify Metadata Recording

**File**: `src/parsers/docling_parser.py`

```python
# In _parse_single_pass, when OCR is enabled
enhanced_metadata = {
    **metadata,
    "parser": "docling",
    "total_pages": page_count or len(page_info) or 0,
    "total_tables": len(table_dicts),
    "text_length": len(full_text),
    "processing_method": "docling_extraction_ocr_enabled",
    # Simplified metadata
    "docling_ocr_mode": "on",
    "docling_ocr_used": True,
    # Quality metrics (informational only)
    "docling_text_chars": len(full_text),
    "docling_word_count": len(re.findall(r'\b\w+\b', full_text)),
    "docling_table_count": len(table_dicts),
}
```

---

## ✅ Verification Checklist

After implementation, verify:

- [ ] All PDFs processed with OCR enabled
- [ ] TableFormer ACCURATE mode always enabled
- [ ] No two-pass logic executed
- [ ] Default behavior is OCR "on" (not "auto")
- [ ] CLI arguments still work but default to "on"
- [ ] Metadata records OCR as used
- [ ] Backward compatibility maintained (old code still works)
- [ ] No performance regressions (timeout handling still works)
- [ ] Text truncation still works (37KB limit)
- [ ] Parallel processing uses OCR "on"

---

## 🧪 Testing Plan

1. **Unit Test**: Process a single PDF, verify OCR is used
2. **Integration Test**: Process 5 PDFs, verify all use OCR
3. **Backward Compat Test**: Old discovery files still process correctly
4. **Performance Test**: Verify timeout handling still works
5. **Metadata Test**: Verify metadata records OCR usage correctly

---

## 📝 Notes

- **Quality metrics functions**: Keep but mark as deprecated/unused (may be useful for future analysis)
- **"auto" mode**: Treat as "on" for backward compatibility
- **CLI arguments**: Keep all arguments but change defaults
- **TableFormer**: Already correct (always ACCURATE), no changes needed

---

## ⚠️ Risks & Mitigation

1. **Risk**: Breaking existing code that relies on "auto" mode
   - **Mitigation**: Treat "auto" as "on" for backward compatibility

2. **Risk**: Performance impact (OCR is slower)
   - **Mitigation**: This is intentional - quality over speed

3. **Risk**: Missing edge cases in reversion
   - **Mitigation**: Keep quality check functions (unused) for reference, thorough testing

4. **Risk**: Breaking parallel processing
   - **Mitigation**: Update both serial and parallel paths consistently



