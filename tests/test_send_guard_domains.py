from src.workflow_7_email_sending.send_guard import _root_domain, check_company_throttle


def test_root_domain_keeps_company_label_for_com_br_domains():
    assert _root_domain("guilherme@incasolar.com.br") == "incasolar.com.br"
    assert _root_domain("https://www.solargroup.com.br/") == "solargroup.com.br"


def test_company_throttle_does_not_match_distinct_com_br_companies():
    record = {
        "kp_email": "guilherme@incasolar.com.br",
        "website": "https://www.incasolar.com.br/",
        "company_name": "INCA SOLAR - Solucões em Energia Fotovoltaica",
        "place_id": "place-a",
    }
    recent_logs = [
        {
            "send_status": "sent",
            "kp_email": "andressa.santos@solargroup.com.br",
            "company_name": "Solar Group - Filial Santana do Parnaíba",
            "place_id": "place-b",
        }
    ]

    assert check_company_throttle(record, recent_logs, send_mode="gmail_api") is None
