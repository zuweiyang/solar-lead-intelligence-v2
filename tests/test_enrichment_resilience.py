"""
Regression tests — enrichment provider rate-limit resilience
=============================================================

Guards against silent quality collapse when Apollo or Hunter returns HTTP 429.
Verifies:
  - _is_rate_limit_error() correctly identifies 429 responses
  - _mark_rate_limited() sets the flag and prints a message (once only)
  - get_enrichment_counters() returns a snapshot (not a reference)
  - _inc() accumulates correctly across multiple calls
"""
import importlib
import sys
import pytest


def _fresh_enricher():
    """Reload the enricher module so each test starts with zeroed counters."""
    mod_name = "src.workflow_5_5_lead_enrichment.enricher"
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    return importlib.import_module(mod_name)


# ---------------------------------------------------------------------------
# _is_rate_limit_error
# ---------------------------------------------------------------------------

class TestIsRateLimitError:
    def test_http_429_response_object(self):
        enricher = _fresh_enricher()
        try:
            import requests
        except ImportError:
            pytest.skip("requests not installed")

        from requests import HTTPError
        from unittest.mock import MagicMock
        exc = HTTPError()
        resp = MagicMock()
        resp.status_code = 429
        exc.response = resp
        assert enricher._is_rate_limit_error(exc) is True

    def test_http_non_429_not_rate_limited(self):
        enricher = _fresh_enricher()
        try:
            import requests
        except ImportError:
            pytest.skip("requests not installed")

        from requests import HTTPError
        from unittest.mock import MagicMock
        exc = HTTPError()
        resp = MagicMock()
        resp.status_code = 403
        exc.response = resp
        assert enricher._is_rate_limit_error(exc) is False

    def test_string_429_in_message_detected(self):
        enricher = _fresh_enricher()
        exc = Exception("HTTP Error 429: Too Many Requests")
        assert enricher._is_rate_limit_error(exc) is True

    def test_generic_exception_not_rate_limited(self):
        enricher = _fresh_enricher()
        exc = ValueError("connection refused")
        assert enricher._is_rate_limit_error(exc) is False


# ---------------------------------------------------------------------------
# _mark_rate_limited + _PROVIDER_RATE_LIMITED flag
# ---------------------------------------------------------------------------

class TestMarkRateLimited:
    def test_marks_provider_as_rate_limited(self, capsys):
        enricher = _fresh_enricher()
        assert not enricher._PROVIDER_RATE_LIMITED.get("apollo")
        enricher._mark_rate_limited("apollo", "example.com")
        assert enricher._PROVIDER_RATE_LIMITED["apollo"] is True

    def test_prints_message_first_time_only(self, capsys):
        enricher = _fresh_enricher()
        enricher._mark_rate_limited("hunter", "a.com")
        enricher._mark_rate_limited("hunter", "b.com")
        out = capsys.readouterr().out
        assert out.count("RATE_LIMITED") == 1

    def test_different_providers_flagged_independently(self):
        enricher = _fresh_enricher()
        enricher._mark_rate_limited("apollo", "x.com")
        assert enricher._PROVIDER_RATE_LIMITED.get("apollo") is True
        assert not enricher._PROVIDER_RATE_LIMITED.get("hunter")


# ---------------------------------------------------------------------------
# _inc + get_enrichment_counters
# ---------------------------------------------------------------------------

class TestEnrichmentCounters:
    def test_inc_accumulates(self):
        enricher = _fresh_enricher()
        enricher._inc("apollo_attempts")
        enricher._inc("apollo_attempts")
        enricher._inc("apollo_ok")
        c = enricher.get_enrichment_counters()
        assert c["apollo_attempts"] == 2
        assert c["apollo_ok"] == 1

    def test_get_counters_returns_snapshot(self):
        enricher = _fresh_enricher()
        enricher._inc("hunter_attempts")
        snap = enricher.get_enrichment_counters()
        enricher._inc("hunter_attempts")
        # snapshot must not change after further _inc calls
        assert snap["hunter_attempts"] == 1

    def test_all_expected_keys_present(self):
        enricher = _fresh_enricher()
        c = enricher.get_enrichment_counters()
        for key in (
            "apollo_attempts", "apollo_ok", "apollo_rate_limited", "apollo_errors",
            "hunter_attempts", "hunter_ok", "hunter_rate_limited", "hunter_errors",
            "website_ok", "guessed_ok", "mock_ok", "none_ok",
        ):
            assert key in c, f"Missing counter: {key}"
