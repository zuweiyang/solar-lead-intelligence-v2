from src.market_localization import (
    BRAZIL_SEARCH_KEYWORDS,
    get_crawl_target_paths,
    get_generic_guess_local_parts,
    get_generic_mailbox_local_parts,
    get_search_keywords,
)
from src.workflow_5_5_lead_enrichment.enricher import _guess_email
from src.workflow_5_9_email_verification.email_verifier import is_generic_mailbox
from src.workflow_9_campaign_runner.campaign_config import CampaignConfig, get_effective_keywords


def test_brazil_uses_portuguese_default_keywords() -> None:
    cfg = CampaignConfig(country="Brazil", keyword_mode="default")
    assert get_effective_keywords(cfg) == list(BRAZIL_SEARCH_KEYWORDS)
    assert "energia solar" in get_search_keywords("Brazil")
    assert "solar installer" not in get_search_keywords("Brazil")


def test_brazil_crawl_paths_prioritize_contact_pages() -> None:
    paths = get_crawl_target_paths("Brazil")
    assert paths[:3] == ["/contato", "/fale-conosco", "/orcamento"]
    assert "/contact" in paths


def test_brazil_guess_email_uses_local_generic_prefixes() -> None:
    first = _guess_email("empresa.com.br", 0, "Brazil")
    second = _guess_email("empresa.com.br", 1, "Brazil")
    third = _guess_email("empresa.com.br", 2, "Brazil")
    assert first["kp_email"] == "contato@empresa.com.br"
    assert second["kp_email"] == "comercial@empresa.com.br"
    assert third["kp_email"] == "vendas@empresa.com.br"
    assert get_generic_guess_local_parts("Brazil")[0] == "contato"


def test_brazil_generic_mailboxes_are_detected() -> None:
    prefixes = get_generic_mailbox_local_parts("Brazil")
    assert "contato" in prefixes
    assert "comercial" in prefixes
    assert "vendas" in prefixes
    assert is_generic_mailbox("contato@empresa.com.br")
    assert is_generic_mailbox("vendas@empresa.com.br")
