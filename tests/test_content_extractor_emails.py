from src.workflow_3_web_crawler.content_extractor import _extract_contacts


def test_extract_contacts_preserves_multi_level_tld_from_visible_text():
    html = """
    <html>
      <body>
        <div>Contato: contato@projetosolarium.com.br</div>
      </body>
    </html>
    """

    emails, phones = _extract_contacts({"home": html})

    assert "contato@projetosolarium.com.br" in emails
    assert "contato@projetosolarium.com" not in emails
    assert phones == []


def test_extract_contacts_preserves_multi_level_tld_from_mailto():
    html = """
    <html>
      <body>
        <a href="mailto:contato@fageletrica.com.br?subject=Oi">Email</a>
      </body>
    </html>
    """

    emails, phones = _extract_contacts({"home": html})

    assert emails[0] == "contato@fageletrica.com.br"
