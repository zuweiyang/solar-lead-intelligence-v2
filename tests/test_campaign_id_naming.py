from __future__ import annotations

import re

from src.workflow_9_campaign_runner.campaign_config import CampaignConfig
from src.workflow_9_campaign_runner.campaign_state import (
    build_campaign_id,
    initialize_campaign_state,
)


def test_build_campaign_id_uses_city_timestamp_and_suffix():
    config = CampaignConfig(city="Los Angeles", region="California", country="United States")

    campaign_id = build_campaign_id(config)

    assert campaign_id.startswith("los-angeles_")
    assert re.fullmatch(r"los-angeles_\d{8}_\d{6}_[0-9a-f]{4}", campaign_id)


def test_build_campaign_id_falls_back_cleanly_when_city_has_unicode():
    config = CampaignConfig(city="Niterói", region="Rio de Janeiro", country="Brazil")

    campaign_id = build_campaign_id(config)

    assert campaign_id.startswith("niteroi_")
    assert re.fullmatch(r"niteroi_\d{8}_\d{6}_[0-9a-f]{4}", campaign_id)


def test_initialize_campaign_state_persists_readable_campaign_id(tmp_path):
    state_path = tmp_path / "campaign_state.json"
    config = CampaignConfig(city="Miami", region="Florida", country="United States")

    state = initialize_campaign_state(config, path=state_path)

    assert state["campaign_id"].startswith("miami_")
    assert re.fullmatch(r"miami_\d{8}_\d{6}_[0-9a-f]{4}", state["campaign_id"])
