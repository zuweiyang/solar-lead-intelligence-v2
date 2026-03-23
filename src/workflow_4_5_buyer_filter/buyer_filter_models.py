# Workflow 4.5 — Buyer Filter: Data Models
#
# BuyerFilterResult holds all structured buyer-filter fields for one company.
# These fields sit between Workflow 4 (company_type classification) and
# Workflow 5 (lead scoring), adding commercial-relevance judgment.

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Value chain role constants
# ---------------------------------------------------------------------------

ROLE_INSTALLER          = "installer"
ROLE_EPC_OR_CONTRACTOR  = "epc_or_contractor"
ROLE_DEVELOPER          = "developer"
ROLE_DISTRIBUTOR        = "distributor"
ROLE_MANUFACTURER       = "manufacturer"
ROLE_CONSULTANT         = "consultant"
ROLE_MEDIA_OR_DIRECTORY = "media_or_directory"
ROLE_ASSOCIATION        = "association_or_nonbuyer"
ROLE_UNCLEAR            = "unclear"

ALL_ROLES = [
    ROLE_INSTALLER,
    ROLE_EPC_OR_CONTRACTOR,
    ROLE_DEVELOPER,
    ROLE_DISTRIBUTOR,
    ROLE_MANUFACTURER,
    ROLE_CONSULTANT,
    ROLE_MEDIA_OR_DIRECTORY,
    ROLE_ASSOCIATION,
    ROLE_UNCLEAR,
]


# ---------------------------------------------------------------------------
# BuyerFilterResult dataclass
# ---------------------------------------------------------------------------

@dataclass
class BuyerFilterResult:
    """
    Structured buyer-filter output for one company.

    Scores are integers in the range 0–10.
    Flags are booleans.
    Reason strings explain every non-obvious decision.

    This structure is serialised to buyer_filter.json alongside all
    fields from company_analysis.json, ready for Workflow 5 (P1-1B) consumption.
    """

    # --- Value chain role ---
    value_chain_role:   str = ROLE_UNCLEAR
    value_chain_reason: str = ""

    # --- Core buyer fit scores (0–10) ---
    buyer_likelihood_score:    int = 0   # overall likelihood of being a real buyer
    procurement_relevance_score: int = 0 # likelihood they procure mounting/storage products
    market_fit_score:          int = 0   # commercial/project market alignment
    project_signal_strength:   int = 0   # evidence of project-oriented activity

    # --- Supporting signal scores (0–10) ---
    commercial_signal_strength:  int = 0
    utility_signal_strength:     int = 0
    installer_signal_strength:   int = 0
    developer_signal_strength:   int = 0
    distributor_signal_strength: int = 0

    # --- Negative targeting flags ---
    competitor_flag:         bool = False   # manufacturer or direct competitor
    manufacturer_flag:       bool = False   # produces products rather than buys
    consultant_flag:         bool = False   # advisory/consulting only
    media_or_directory_flag: bool = False   # blog, aggregator, directory, press
    negative_residential_flag: bool = False # primarily homeowner/residential focus

    # --- Auditability ---
    buyer_filter_reason:       str        = ""
    negative_targeting_reasons: list[str] = field(default_factory=list)

    # ---------------------------------------------------------------------------
    # Serialisation helpers
    # ---------------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Return a flat dict suitable for JSON serialisation and CSV rows."""
        return {
            "value_chain_role":             self.value_chain_role,
            "value_chain_reason":           self.value_chain_reason,
            "buyer_likelihood_score":       self.buyer_likelihood_score,
            "procurement_relevance_score":  self.procurement_relevance_score,
            "market_fit_score":             self.market_fit_score,
            "project_signal_strength":      self.project_signal_strength,
            "commercial_signal_strength":   self.commercial_signal_strength,
            "utility_signal_strength":      self.utility_signal_strength,
            "installer_signal_strength":    self.installer_signal_strength,
            "developer_signal_strength":    self.developer_signal_strength,
            "distributor_signal_strength":  self.distributor_signal_strength,
            "competitor_flag":              self.competitor_flag,
            "manufacturer_flag":            self.manufacturer_flag,
            "consultant_flag":              self.consultant_flag,
            "media_or_directory_flag":      self.media_or_directory_flag,
            "negative_residential_flag":    self.negative_residential_flag,
            "buyer_filter_reason":          self.buyer_filter_reason,
            "negative_targeting_reasons":   self.negative_targeting_reasons,
        }


# ---------------------------------------------------------------------------
# BUYER_FILTER_FIELDS — ordered list used for CSV column headers
# ---------------------------------------------------------------------------

BUYER_FILTER_FIELDS: list[str] = [
    "value_chain_role",
    "value_chain_reason",
    "buyer_likelihood_score",
    "procurement_relevance_score",
    "market_fit_score",
    "project_signal_strength",
    "commercial_signal_strength",
    "utility_signal_strength",
    "installer_signal_strength",
    "developer_signal_strength",
    "distributor_signal_strength",
    "competitor_flag",
    "manufacturer_flag",
    "consultant_flag",
    "media_or_directory_flag",
    "negative_residential_flag",
    "buyer_filter_reason",
    "negative_targeting_reasons",
]
