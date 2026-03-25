from src.workflow_3_web_crawler.content_extractor import _extract_contacts


def test_extract_contacts_preserves_multi_level_tld_from_visible_text():
    html = """
    <html>
      <body>
        <div>Contato: contato@projetosolarium.com.br</div>
      </body>
    </html>
    """

    emails, phones, whatsapp_phones = _extract_contacts({"home": html})

    assert "contato@projetosolarium.com.br" in emails
    assert "contato@projetosolarium.com" not in emails
    assert phones == []
    assert whatsapp_phones == []


def test_extract_contacts_preserves_multi_level_tld_from_mailto():
    html = """
    <html>
      <body>
        <a href="mailto:contato@fageletrica.com.br?subject=Oi">Email</a>
      </body>
    </html>
    """

    emails, phones, whatsapp_phones = _extract_contacts({"home": html})

    assert emails[0] == "contato@fageletrica.com.br"
    assert phones == []
    assert whatsapp_phones == []


def test_extract_contacts_preserves_brazil_phone_and_whatsapp_link():
    html = """
    <html>
      <body>
        <div>WhatsApp: (11) 97071-3044</div>
        <a href="https://wa.me/5511970713044">Fale conosco</a>
      </body>
    </html>
    """

    emails, phones, whatsapp_phones = _extract_contacts({"home": html})

    assert emails == []
    assert "(11) 97071-3044" in phones
    assert "+5511970713044" in whatsapp_phones


def test_extract_contacts_marks_phone_as_whatsapp_when_page_explicitly_mentions_whatsapp():
    html = """
    <html>
      <body>
        <div>WhatsApp comercial</div>
        <div>Telefone: +1 604-555-0101</div>
      </body>
    </html>
    """

    emails, phones, whatsapp_phones = _extract_contacts({"home": html})

    assert emails == []
    assert "+1 604-555-0101" in phones
    assert "+1 604-555-0101" in whatsapp_phones
