# Workflow 1: Lead Generation
# Generates industry keywords and geographic locations for search tasks.

SOLAR_KEYWORDS = [
    "solar installer",
    "solar EPC",
    "solar contractor",
    "solar developer",
    "solar energy company",
    "solar panel installer",
    "commercial solar installer",
    "energy storage integrator",
    "BESS integrator",
]

US_STATES = [
    "California", "Texas", "Florida", "Arizona", "Nevada",
    "New York", "New Jersey", "Massachusetts", "Colorado", "North Carolina",
    "Georgia", "Virginia", "Maryland", "Illinois", "Minnesota",
    "Oregon", "Washington", "Ohio", "Michigan", "Pennsylvania",
]

CANADIAN_PROVINCES = [
    "Ontario", "British Columbia", "Alberta", "Quebec",
]


def generate_keywords() -> list[str]:
    """Return the list of solar industry search keywords."""
    return SOLAR_KEYWORDS


def generate_locations() -> list[str]:
    """Return the combined list of US states and Canadian provinces."""
    return US_STATES + CANADIAN_PROVINCES
