# Workflow 7.5: Engagement Tracking — Email Tracking Injector
# Injects open-tracking pixel and click-tracking URLs into email HTML.
#
# IMPORTANT LIMITATIONS:
# - Open tracking is best-effort only.
# - Apple Mail Privacy Protection, Gmail image proxy, and corporate email
#   security gateways may pre-fetch pixels, causing inflated open counts.
# - Click tracking is more reliable than open tracking.
# - Reply tracking is NOT part of this module.

import re
from urllib.parse import quote, urlparse

_LINK_RE = re.compile(
    r'href=["\'](?P<url>https?://[^"\'>\s]+)["\']',
    re.IGNORECASE,
)

_PLAIN_URL_RE = re.compile(
    r'(?<!["\'])(?P<url>https?://[^\s<>"\']+)',
)


def build_html_email(email_body: str) -> str:
    """
    Wrap plain-text email body in minimal valid HTML.
    Converts paragraphs and detects bare URLs for clickable links.
    """
    paragraphs = [p.strip() for p in email_body.strip().split("\n\n") if p.strip()]
    html_parts: list[str] = []
    for para in paragraphs:
        lines = para.replace("\n", "<br>\n")
        lines = _PLAIN_URL_RE.sub(
            lambda m: f'<a href="{m.group("url")}">{m.group("url")}</a>',
            lines,
        )
        html_parts.append(f"<p>{lines}</p>")

    body_html = "\n".join(html_parts)
    return (
        "<!DOCTYPE html>\n"
        "<html><head><meta charset='utf-8'></head>\n"
        f"<body>\n{body_html}\n</body></html>"
    )


def inject_open_tracking(html: str, tracking_id: str, tracking_base_url: str) -> str:
    """
    Inject a 1x1 transparent tracking pixel before </body>.
    If </body> not found, appends to end.
    """
    pixel_url = f"{tracking_base_url.rstrip('/')}/track/open/{tracking_id}"
    pixel_tag = (
        f'<img src="{pixel_url}" width="1" height="1" '
        f'style="display:none;border:0;" alt="" />'
    )
    idx = html.lower().rfind("</body>")
    if idx >= 0:
        return html[:idx] + pixel_tag + "\n" + html[idx:]
    return html + "\n" + pixel_tag


def rewrite_click_links(html: str, tracking_id: str, tracking_base_url: str) -> tuple[str, int]:
    """
    Replace http/https href links in HTML with click-tracking URLs.
    Returns (modified_html, count_of_links_rewritten).
    Skips already-tracking URLs.
    """
    base = tracking_base_url.rstrip("/")
    track_prefix = f"{base}/track/"
    count = 0

    def _replace(m: re.Match) -> str:
        nonlocal count
        original_url = m.group("url")
        if original_url.startswith(track_prefix):
            return m.group(0)
        try:
            parsed = urlparse(original_url)
            if parsed.scheme not in ("http", "https"):
                return m.group(0)
        except Exception:
            return m.group(0)
        encoded  = quote(original_url, safe="")
        new_url  = f"{base}/track/click/{tracking_id}?url={encoded}"
        count   += 1
        orig     = m.group(0)
        qchar    = orig[5]  # href=" or href='
        return f"href={qchar}{new_url}{qchar}"

    modified = _LINK_RE.sub(_replace, html)
    return modified, count


def prepare_tracked_email(
    email_body: str,
    tracking_id: str,
    tracking_base_url: str,
) -> dict:
    """
    Full pipeline: plain text → HTML → inject pixel → rewrite links.

    Returns:
        plain_text, html_body, tracking_id, tracked_links_count
    """
    plain_text = email_body
    html = build_html_email(email_body)
    html = inject_open_tracking(html, tracking_id, tracking_base_url)
    html, link_count = rewrite_click_links(html, tracking_id, tracking_base_url)
    return {
        "plain_text":          plain_text,
        "html_body":           html,
        "tracking_id":         tracking_id,
        "tracked_links_count": link_count,
    }
