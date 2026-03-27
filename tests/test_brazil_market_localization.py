from src.market_localization import (
    BRAZIL_CRAWL_HOME_HINTS,
    BRAZIL_CRAWL_ACCEPT_LANGUAGE,
    BRAZIL_MARKET_PROFILE,
    BRAZIL_SEARCH_KEYWORDS,
    get_crawl_accept_language,
    get_crawl_home_hints,
    get_email_language,
    get_market_profile,
    get_crawl_target_paths,
    get_generic_guess_local_parts,
    get_generic_mailbox_local_parts,
    get_search_keywords,
)
from src.workflow_6_email_generation.email_generator import _greeting, _prompt_localization
from src.workflow_6_email_generation.email_templates import build_rule_based_email
from src.workflow_5_5_lead_enrichment.enricher import _guess_email
from src.workflow_5_9_email_verification.email_verifier import is_generic_mailbox
from src.workflow_9_campaign_runner.campaign_config import CampaignConfig, get_effective_keywords


def test_brazil_uses_portuguese_default_keywords() -> None:
    cfg = CampaignConfig(country="Brazil", keyword_mode="default")
    assert get_effective_keywords(cfg) == list(BRAZIL_SEARCH_KEYWORDS)
    assert "integrador solar" in get_search_keywords("Brazil")
    assert "distribuidor fotovoltaico" in get_search_keywords("Brazil")
    assert "empresa EPC solar" in get_search_keywords("Brazil")
    assert "empresa de energia solar comercial" in get_search_keywords("Brazil")
    assert len(get_search_keywords("Brazil")) == 6
    assert "solar installer" not in get_search_keywords("Brazil")


def test_brazil_crawl_paths_prioritize_contact_pages() -> None:
    paths = get_crawl_target_paths("Brazil")
    assert paths[:3] == ["/contato", "/fale-conosco", "/orcamento"]
    assert "/contact" in paths


def test_brazil_crawl_home_hints_prioritize_localized_site_versions() -> None:
    assert get_crawl_home_hints("Brazil") == list(BRAZIL_CRAWL_HOME_HINTS)


def test_brazil_market_profile_is_country_template() -> None:
    profile = get_market_profile("Brazil")
    assert profile == BRAZIL_MARKET_PROFILE
    assert profile.country == "Brazil"
    assert profile.email_language == "pt-BR"
    assert profile.email_language_name == "Brazilian Portuguese"
    assert profile.search_keywords[0] == "integrador solar"
    assert "/contato" in profile.crawl_target_paths
    assert "/pt-br" in profile.crawl_home_hints


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


def test_brazil_email_language_defaults_to_portuguese() -> None:
    assert get_email_language("Brazil") == "pt-BR"


def test_brazil_crawl_accept_language_defaults_to_portuguese() -> None:
    assert get_crawl_accept_language("Brazil") == BRAZIL_CRAWL_ACCEPT_LANGUAGE


def test_brazil_rule_based_email_is_portuguese() -> None:
    record = {
        "country": "Brazil",
        "company_name": "Impla Solar",
        "kp_name": "",
        "lead_score": 65,
        "email_angle": "installation",
        "market_focus": "commercial",
    }
    draft = build_rule_based_email(record)
    assert "Pergunta rápida" in draft["subject"]
    assert "Olá equipe da Impla Solar" in draft["body"]
    assert "Atenciosamente," in draft["body"]
    assert "Quick question" not in draft["subject"]
    assert "Hello Impla Solar team," not in draft["body"]


def test_brazil_email_prompt_requests_portuguese_output() -> None:
    record = {
        "country": "Brazil",
        "company_name": "Marsol Energia Solar",
    }
    prompt_loc = _prompt_localization(record)
    assert prompt_loc["preferred_language"] == "Brazilian Portuguese"
    assert _greeting(record).startswith("Olá equipe da")
