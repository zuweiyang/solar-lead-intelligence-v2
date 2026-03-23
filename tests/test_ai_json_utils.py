"""
Regression tests — ai_json_utils
=================================

Covers every failure class that parse_ai_json() and validate_required_keys()
can produce, plus the classify_parse_failure() helper.
"""
import json
import pytest

from src.workflow_6_email_generation.ai_json_utils import (
    AIParseError,
    parse_ai_json,
    validate_required_keys,
    classify_parse_failure,
    fix_json_control_chars,
)


# ---------------------------------------------------------------------------
# fix_json_control_chars
# ---------------------------------------------------------------------------

class TestFixJsonControlChars:
    def test_bare_newline_inside_string_escaped(self):
        raw = '{"body": "line1\nline2"}'
        fixed = fix_json_control_chars(raw)
        assert json.loads(fixed)["body"] == "line1\nline2"

    def test_bare_tab_inside_string_escaped(self):
        raw = '{"body": "col1\tcol2"}'
        fixed = fix_json_control_chars(raw)
        assert json.loads(fixed)["body"] == "col1\tcol2"

    def test_escaped_backslash_not_mangled(self):
        raw = r'{"path": "C:\\Users\\foo"}'
        fixed = fix_json_control_chars(raw)
        assert json.loads(fixed)["path"] == r"C:\Users\foo"

    def test_newline_outside_string_passed_through(self):
        raw = '{\n"k": "v"\n}'
        fixed = fix_json_control_chars(raw)
        assert json.loads(fixed)["k"] == "v"


# ---------------------------------------------------------------------------
# parse_ai_json — success paths
# ---------------------------------------------------------------------------

class TestParseAiJsonSuccess:
    def test_clean_json(self):
        result = parse_ai_json('{"a": 1}')
        assert result == {"a": 1}

    def test_markdown_fence_stripped(self):
        result = parse_ai_json("```json\n{\"x\": 2}\n```")
        assert result["x"] == 2

    def test_bare_markdown_fence_stripped(self):
        result = parse_ai_json("```\n{\"x\": 3}\n```")
        assert result["x"] == 3

    def test_trailing_prose_extracted(self):
        raw = '{"score": 80} Here is my reasoning...'
        result = parse_ai_json(raw)
        assert result["score"] == 80

    def test_bare_newlines_inside_string_fixed(self):
        raw = '{"body": "hello\nworld"}'
        result = parse_ai_json(raw)
        assert "hello" in result["body"]


# ---------------------------------------------------------------------------
# parse_ai_json — failure paths (tagged error messages)
# ---------------------------------------------------------------------------

class TestParseAiJsonFailures:
    def test_empty_response_raises(self):
        with pytest.raises(json.JSONDecodeError) as exc_info:
            parse_ai_json("")
        assert "[empty_response]" in str(exc_info.value)

    def test_whitespace_only_raises(self):
        with pytest.raises(json.JSONDecodeError) as exc_info:
            parse_ai_json("   \n  ")
        assert "[empty_response]" in str(exc_info.value)

    def test_truncated_response_raises(self):
        with pytest.raises(json.JSONDecodeError) as exc_info:
            parse_ai_json('{"key": "value that never ends')
        assert "[truncated]" in str(exc_info.value)

    def test_malformed_json_raises(self):
        with pytest.raises(json.JSONDecodeError) as exc_info:
            parse_ai_json("{not valid json at all!!!}")
        assert "[malformed_json]" in str(exc_info.value)

    def test_context_included_in_error(self):
        with pytest.raises(json.JSONDecodeError) as exc_info:
            parse_ai_json("", context="Acme Corp")
        assert "Acme Corp" in str(exc_info.value)


# ---------------------------------------------------------------------------
# validate_required_keys
# ---------------------------------------------------------------------------

class TestValidateRequiredKeys:
    def test_all_present_no_raise(self):
        validate_required_keys({"a": 1, "b": 2}, ["a", "b"])  # should not raise

    def test_missing_key_raises_ai_parse_error(self):
        with pytest.raises(AIParseError) as exc_info:
            validate_required_keys({"a": 1}, ["a", "b"], context="TestCo")
        err = exc_info.value
        assert err.failure_class == "missing_keys"
        assert "b" in str(err)
        assert "TestCo" in str(err)

    def test_falsy_value_counts_as_missing(self):
        with pytest.raises(AIParseError):
            validate_required_keys({"a": 0, "b": ""}, ["a", "b"])

    def test_context_optional(self):
        with pytest.raises(AIParseError):
            validate_required_keys({}, ["required_field"])


# ---------------------------------------------------------------------------
# classify_parse_failure
# ---------------------------------------------------------------------------

class TestClassifyParseFailure:
    def _empty_exc(self):
        try:
            parse_ai_json("")
        except json.JSONDecodeError as exc:
            return exc

    def _truncated_exc(self):
        try:
            parse_ai_json('{"x": "never closed')
        except json.JSONDecodeError as exc:
            return exc

    def _malformed_exc(self):
        try:
            parse_ai_json("{!!!}")
        except json.JSONDecodeError as exc:
            return exc

    def test_classify_empty_response(self):
        assert classify_parse_failure(self._empty_exc()) == "empty_response"

    def test_classify_truncated(self):
        assert classify_parse_failure(self._truncated_exc()) == "truncated"

    def test_classify_malformed_json(self):
        assert classify_parse_failure(self._malformed_exc()) == "malformed_json"

    def test_classify_missing_keys(self):
        try:
            validate_required_keys({}, ["x"])
        except AIParseError as exc:
            # AIParseError is not a JSONDecodeError — caller's except block catches it
            assert exc.failure_class == "missing_keys"

    def test_classify_unknown_exception(self):
        assert classify_parse_failure(ValueError("something else")) == "unknown"

    def test_classify_requests_timeout(self):
        try:
            import requests
        except ImportError:
            pytest.skip("requests not installed")
        exc = requests.exceptions.Timeout()
        assert classify_parse_failure(exc) == "timeout"

    def test_classify_requests_http_error(self):
        try:
            import requests
        except ImportError:
            pytest.skip("requests not installed")
        exc = requests.exceptions.HTTPError()
        assert classify_parse_failure(exc) == "http_error"
