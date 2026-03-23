"""
Regression tests — signal_collector URL hygiene
================================================

Guards against junk links (share buttons, javascript: URIs, scheme-less URLs)
contaminating the social link list collected during signal research.

Covers:
  - _normalize_url()   — scheme normalisation and rejection
  - _is_share_link()   — share-button / intent URL detection
  - _is_valid_social_url() — combined gate
  - extract_social_links() — full HTML extraction with counts
"""
import pytest

from src.workflow_5_8_signal_research.signal_collector import (
    _normalize_url,
    _is_share_link,
    _is_valid_social_url,
    extract_social_links,
)


# ---------------------------------------------------------------------------
# _normalize_url
# ---------------------------------------------------------------------------

class TestNormalizeUrl:
    def test_https_url_unchanged(self):
        assert _normalize_url("https://linkedin.com/company/acme") == \
            "https://linkedin.com/company/acme"

    def test_http_url_unchanged(self):
        assert _normalize_url("http://facebook.com/acme") == "http://facebook.com/acme"

    def test_scheme_less_becomes_https(self):
        assert _normalize_url("//linkedin.com/company/acme") == \
            "https://linkedin.com/company/acme"

    def test_javascript_uri_rejected(self):
        assert _normalize_url("javascript:void(0)") == ""

    def test_mailto_rejected(self):
        assert _normalize_url("mailto:hello@example.com") == ""

    def test_tel_rejected(self):
        assert _normalize_url("tel:+15551234567") == ""

    def test_data_uri_rejected(self):
        assert _normalize_url("data:text/plain;base64,SGVsbG8=") == ""

    def test_relative_path_rejected(self):
        assert _normalize_url("/about") == ""

    def test_empty_string_rejected(self):
        assert _normalize_url("") == ""

    def test_whitespace_stripped(self):
        result = _normalize_url("  https://linkedin.com/company/acme  ")
        assert result == "https://linkedin.com/company/acme"


# ---------------------------------------------------------------------------
# _is_share_link
# ---------------------------------------------------------------------------

class TestIsShareLink:
    def test_facebook_sharer_detected(self):
        assert _is_share_link("https://www.facebook.com/sharer/sharer.php?u=https://example.com")

    def test_twitter_intent_tweet_detected(self):
        assert _is_share_link("https://twitter.com/intent/tweet?text=Hello")

    def test_linkedin_share_article_detected(self):
        assert _is_share_link("https://www.linkedin.com/shareArticle?mini=true&url=https://example.com")

    def test_facebook_company_page_not_share(self):
        assert not _is_share_link("https://www.facebook.com/acmesolar")

    def test_linkedin_company_page_not_share(self):
        assert not _is_share_link("https://www.linkedin.com/company/acme-solar")

    def test_instagram_profile_not_share(self):
        assert not _is_share_link("https://www.instagram.com/acmesolar/")

    def test_youtube_channel_not_share(self):
        assert not _is_share_link("https://www.youtube.com/channel/UCxxxxxx")

    def test_signup_page_detected(self):
        assert _is_share_link("https://www.linkedin.com/signup")

    def test_login_page_detected(self):
        assert _is_share_link("https://www.facebook.com/login")


# ---------------------------------------------------------------------------
# _is_valid_social_url
# ---------------------------------------------------------------------------

class TestIsValidSocialUrl:
    def test_valid_linkedin_company(self):
        assert _is_valid_social_url("https://www.linkedin.com/company/acme-solar")

    def test_valid_facebook_page(self):
        assert _is_valid_social_url("https://www.facebook.com/AcmeSolarEnergy")

    def test_empty_rejected(self):
        assert not _is_valid_social_url("")

    def test_javascript_rejected(self):
        assert not _is_valid_social_url("javascript:void(0)")

    def test_share_link_rejected(self):
        assert not _is_valid_social_url(
            "https://www.facebook.com/sharer/sharer.php?u=https://example.com"
        )

    def test_scheme_less_url_accepted_after_normalisation(self):
        assert _is_valid_social_url("//linkedin.com/company/test-co")


# ---------------------------------------------------------------------------
# extract_social_links — full HTML extraction
# ---------------------------------------------------------------------------

class TestExtractSocialLinks:
    def _make_html(self, hrefs: list[str]) -> str:
        links = "\n".join(f'<a href="{h}">link</a>' for h in hrefs)
        return f"<html><body>{links}</body></html>"

    def test_valid_company_links_accepted(self):
        html = self._make_html([
            "https://www.linkedin.com/company/acme-solar",
            "https://www.facebook.com/acmesolar",
        ])
        links, counts = extract_social_links(html)
        assert len(links) == 2
        assert counts["accepted"] == 2
        assert counts["skipped"] == 0
        assert counts["extracted"] == 2

    def test_share_links_skipped(self):
        # Facebook share URL contains "facebook.com" and is matched by SOCIAL_DOMAINS,
        # then rejected by _is_share_link() (path contains /sharer).
        # LinkedIn share URL lacks "/company" so it never matches SOCIAL_DOMAINS at all
        # (not counted as extracted, not counted as skipped — it is invisible to the extractor).
        html = self._make_html([
            "https://www.linkedin.com/shareArticle?mini=true&url=https://example.com",
            "https://www.facebook.com/sharer/sharer.php?u=https://example.com",
        ])
        links, counts = extract_social_links(html)
        assert len(links) == 0
        assert counts["skipped"] == 1   # only facebook sharer matched + was rejected
        assert counts["accepted"] == 0

    def test_javascript_hrefs_skipped(self):
        html = self._make_html([
            "javascript:window.open('https://www.linkedin.com/company/acme')",
        ])
        # javascript: hrefs won't match the domain check before normalisation,
        # so they won't appear in extracted count — but they must not appear in accepted.
        links, counts = extract_social_links(html)
        assert len(links) == 0

    def test_scheme_less_links_normalised(self):
        html = self._make_html([
            "//www.linkedin.com/company/scheme-less-co",
        ])
        links, counts = extract_social_links(html)
        assert len(links) == 1
        assert links[0]["url"].startswith("https://")
        assert counts["normalized"] == 1
        assert counts["accepted"] == 1

    def test_mix_of_valid_and_share_links(self):
        html = self._make_html([
            "https://www.linkedin.com/company/real-co",
            "https://www.facebook.com/sharer/sharer.php?u=https://example.com",
            "https://www.facebook.com/RealCompanyPage",
            "https://www.instagram.com/realco/",
        ])
        links, counts = extract_social_links(html)
        platforms = {l["platform"] for l in links}
        assert "linkedin" in platforms
        assert "facebook" in platforms
        assert counts["skipped"] >= 1   # the share link was skipped

    def test_returns_tuple_with_counts(self):
        html = self._make_html(["https://www.linkedin.com/company/x"])
        result = extract_social_links(html)
        assert isinstance(result, tuple) and len(result) == 2
        links, counts = result
        assert isinstance(links, list)
        assert isinstance(counts, dict)
        assert "extracted" in counts and "accepted" in counts

    def test_no_social_links_in_html(self):
        html = "<html><body><a href='/about'>About</a></body></html>"
        links, counts = extract_social_links(html)
        assert links == []
        assert counts["extracted"] == 0
        assert counts["accepted"] == 0
