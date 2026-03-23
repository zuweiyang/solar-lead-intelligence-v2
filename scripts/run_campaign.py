"""
Workflow 9 — Campaign Runner: CLI Entry Point

Examples:
    py scripts/run_campaign.py --city Vancouver --country Canada
    py scripts/run_campaign.py --city Seattle --country USA --company-limit 20
    py scripts/run_campaign.py --city Vancouver --country Canada --run-until email_generation
    py scripts/run_campaign.py --resume
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_9_campaign_runner.campaign_config import CampaignConfig, PIPELINE_STEPS
from src.workflow_9_campaign_runner.campaign_runner import run_campaign, resume_campaign


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Solar Lead Intelligence — Campaign Runner (Workflow 9)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  py scripts/run_campaign.py --city Vancouver --country Canada
  py scripts/run_campaign.py --city Seattle   --country USA --company-limit 20
  py scripts/run_campaign.py --city Vancouver --run-until email_generation
  py scripts/run_campaign.py --resume

Valid --run-until values:
  """ + ", ".join(PIPELINE_STEPS),
    )

    parser.add_argument("--country",       default="Canada",  help="Target country")
    parser.add_argument("--region",        default="",        help="Target region / province / state")
    parser.add_argument("--city",          default="",        help="Target city")
    parser.add_argument("--keyword-mode",  default="default", choices=["default", "custom"],
                        help="'default' uses built-in solar keywords; 'custom' reads --keywords")
    parser.add_argument("--keywords",      nargs="+",         default=[],
                        help="Custom keyword list (used when --keyword-mode custom)")
    parser.add_argument("--company-limit", type=int, default=0,
                        help="Max companies to analyse/score (0 = no limit)")
    parser.add_argument("--crawl-limit",   type=int, default=0,
                        help="Max websites to crawl (0 = no limit)")
    parser.add_argument("--enrich-limit",  type=int, default=0,
                        help="Max leads to enrich / generate emails for (0 = no limit)")
    parser.add_argument("--send-mode",     default="dry_run", choices=["dry_run", "smtp"],
                        help="'dry_run' simulates sending; 'smtp' sends real emails")
    parser.add_argument("--run-until",     default="campaign_status",
                        choices=PIPELINE_STEPS,
                        help="Stop after this step (default: campaign_status = full pipeline)")
    parser.add_argument("--resume",        action="store_true",
                        help="Resume the last interrupted campaign run")
    parser.add_argument("--dry-run",       action="store_true", default=True,
                        help="Mark this run as dry-run (default: True; use --send-mode smtp to send)")

    args = parser.parse_args()

    if args.resume:
        result = resume_campaign()
    else:
        config = CampaignConfig(
            country       = args.country,
            region        = args.region,
            city          = args.city,
            keyword_mode  = args.keyword_mode,
            keywords      = args.keywords,
            company_limit = args.company_limit,
            crawl_limit   = args.crawl_limit,
            enrich_limit  = args.enrich_limit,
            send_mode     = args.send_mode,
            run_until     = args.run_until,
            resume        = False,
            dry_run       = args.dry_run,
        )
        result = run_campaign(config)

    # Exit with non-zero code on failure
    if result.get("status") == "failed":
        print(f"\nCampaign failed at step: {result.get('last_completed_step')}")
        print(f"Error: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
