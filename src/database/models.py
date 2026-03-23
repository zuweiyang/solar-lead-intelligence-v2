# Database: Models
# Dataclass definitions for all core entities in the pipeline.

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class SearchTask:
    keyword:  str
    location: str
    industry: str
    query:    str
    status:   str = "pending"  # pending | done | error


@dataclass
class RawLead:
    company_name:    str
    website:         str
    address:         str = ""
    phone:           str = ""
    rating:          str = ""
    category:        str = ""
    source_keyword:  str = ""
    source_location: str = ""


@dataclass
class CompanyContent:
    company_name:      str
    website:           str
    company_description: str = ""
    services:          str = ""
    industry_keywords: list[str] = field(default_factory=list)


@dataclass
class CompanyProfile:
    company_name:            str
    website:                 str
    address:                 str = ""
    phone:                   str = ""
    rating:                  str = ""
    category:                str = ""
    company_summary:         str = ""
    business_type:           str = ""
    products:                list[str] = field(default_factory=list)
    target_market:           str = ""
    location:                str = ""
    employee_count_estimate: str = "unknown"


@dataclass
class QualifiedLead(CompanyProfile):
    score:           int = 0
    grade:           str = "D"   # A | B | C | D
    score_breakdown: list[str] = field(default_factory=list)


@dataclass
class EmailTemplate:
    company_name:           str
    website:                str
    grade:                  str
    subject:                str = ""
    body:                   str = ""
    follow_up_subject:      str = ""
    follow_up_body:         str = ""
    final_follow_up_subject: str = ""
    final_follow_up_body:   str = ""


@dataclass
class EmailLog:
    date:         str
    company_name: str
    website:      str
    subject:      str
    status:       str   # sent | error | bounced | opened
    error:        str = ""


@dataclass
class CRMRecord:
    company_name:      str
    website:           str
    email:             str = ""
    status:            str = "contacted"  # contacted | replied | interested | meeting_scheduled | unsubscribed
    first_sent_date:   str = ""
    last_contact_date: str = ""
    followup_step:     int = 0
    notes:             str = ""
