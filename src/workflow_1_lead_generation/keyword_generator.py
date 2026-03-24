# Workflow 1: Lead Generation
# Generates industry keywords and geographic locations for search tasks.

from src.market_localization import DEFAULT_SEARCH_KEYWORDS, get_search_keywords

SOLAR_KEYWORDS = list(DEFAULT_SEARCH_KEYWORDS)

US_STATES = [
    "California", "Texas", "Florida", "Arizona", "Nevada",
    "New York", "New Jersey", "Massachusetts", "Colorado", "North Carolina",
    "Georgia", "Virginia", "Maryland", "Illinois", "Minnesota",
    "Oregon", "Washington", "Ohio", "Michigan", "Pennsylvania",
]

CANADIAN_PROVINCES = [
    "Ontario", "British Columbia", "Alberta", "Quebec",
]


def generate_keywords(country: str = "") -> list[str]:
    """Return the list of solar industry search keywords."""
    return get_search_keywords(country)


def generate_locations() -> list[str]:
    """Return the combined list of US states and Canadian provinces."""
    return US_STATES + CANADIAN_PROVINCES
