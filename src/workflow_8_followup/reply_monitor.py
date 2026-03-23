# Workflow 8: Follow-up Automation
# Monitors the inbox for replies and updates lead status in the CRM.

import csv
import imaplib
import email as email_lib
from datetime import date
from config.settings import (
    EMAIL_HOST, EMAIL_ADDRESS, EMAIL_PASSWORD, CRM_DATABASE_FILE,
)

CRM_FIELDS = [
    "company_name", "website", "email", "status",
    "first_sent_date", "last_contact_date", "followup_step", "notes",
]

REPLY_STATUS = "replied"


def connect_imap() -> imaplib.IMAP4_SSL:
    """Connect and authenticate to the IMAP inbox."""
    imap = imaplib.IMAP4_SSL(EMAIL_HOST.replace("smtp.", "imap."))
    imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    return imap


def fetch_reply_senders(since_days: int = 7) -> set[str]:
    """
    Return the set of sender addresses that replied in the last N days.

    TODO: Improve detection by matching email thread subjects/IDs
    rather than sender address alone.
    """
    try:
        imap = connect_imap()
        imap.select("INBOX")
        since_date = (date.today()).strftime("%d-%b-%Y")
        _, message_ids = imap.search(None, f'(SINCE "{since_date}")')
        senders: set[str] = set()

        for mid in message_ids[0].split():
            _, data = imap.fetch(mid, "(RFC822)")
            msg = email_lib.message_from_bytes(data[0][1])
            from_header = msg.get("From", "")
            # Extract email address from "Name <addr>" format
            if "<" in from_header:
                addr = from_header.split("<")[1].rstrip(">").strip().lower()
            else:
                addr = from_header.strip().lower()
            senders.add(addr)

        imap.logout()
        return senders
    except Exception as exc:
        print(f"[Workflow 8] IMAP error: {exc}")
        return set()


def update_crm_status(replied_emails: set[str]) -> int:
    """
    Mark CRM records as 'replied' if their email address is in replied_emails.
    Returns count of records updated.
    """
    try:
        with open(CRM_DATABASE_FILE, newline="", encoding="utf-8") as f:
            records = list(csv.DictReader(f))
    except FileNotFoundError:
        print("[Workflow 8] CRM database not found.")
        return 0

    updated = 0
    for record in records:
        addr = record.get("email", "").strip().lower()
        if addr in replied_emails and record.get("status") != REPLY_STATUS:
            record["status"] = REPLY_STATUS
            record["last_contact_date"] = str(date.today())
            updated += 1

    if updated:
        with open(CRM_DATABASE_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CRM_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        print(f"[Workflow 8] Updated {updated} CRM records to '{REPLY_STATUS}'.")

    return updated


def run() -> int:
    senders = fetch_reply_senders()
    return update_crm_status(senders)


if __name__ == "__main__":
    run()
