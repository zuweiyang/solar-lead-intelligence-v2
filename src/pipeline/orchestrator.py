# Pipeline: Orchestrator
# Runs the full 8-workflow pipeline end-to-end, or individual stages on demand.

import argparse
import sys


def run_workflow_1() -> list:
    from src.workflow_1_lead_generation.search_task_builder import run
    print("\n=== Workflow 1: Lead Generation ===")
    return run()


def run_workflow_2() -> list:
    from src.workflow_2_data_scraping.google_maps_scraper import run as scrape
    from src.workflow_2_data_scraping.data_cleaner import run as clean
    print("\n=== Workflow 2: Data Scraping ===")
    scrape()
    return clean()


def run_workflow_3() -> list:
    from src.workflow_3_website_crawling.website_crawler import run as crawl
    from src.workflow_3_website_crawling.content_extractor import run as extract
    print("\n=== Workflow 3: Website Crawling ===")
    crawl_results = crawl()
    return extract(crawl_results)


def run_workflow_4() -> list:
    from src.workflow_4_ai_research.company_analyzer import run as analyze
    from src.workflow_4_ai_research.profile_builder import run as build
    print("\n=== Workflow 4: AI Company Research ===")
    analyses = analyze()
    return build(analyses)


def run_workflow_5() -> list:
    from src.workflow_5_lead_qualification.lead_classifier import run
    print("\n=== Workflow 5: Lead Qualification ===")
    return run()


def run_workflow_6() -> list:
    from src.workflow_6_email_personalization.email_generator import run
    print("\n=== Workflow 6: Email Personalization ===")
    return run()


def run_workflow_7() -> None:
    from src.workflow_7_email_sending.send_scheduler import run_scheduled_batch
    print("\n=== Workflow 7: Email Sending ===")
    run_scheduled_batch()


def run_workflow_8() -> None:
    from src.workflow_8_followup.followup_manager import run as check_followups
    from src.workflow_8_followup.reply_monitor import run as check_replies
    print("\n=== Workflow 8: Follow-up & Reply Monitoring ===")
    check_replies()
    check_followups()


WORKFLOW_MAP = {
    "1": run_workflow_1,
    "2": run_workflow_2,
    "3": run_workflow_3,
    "4": run_workflow_4,
    "5": run_workflow_5,
    "6": run_workflow_6,
    "7": run_workflow_7,
    "8": run_workflow_8,
}


def run_full_pipeline() -> None:
    """Execute all 8 workflows in sequence."""
    print("=== Starting Full Pipeline ===")
    run_workflow_1()
    run_workflow_2()
    run_workflow_3()
    run_workflow_4()
    run_workflow_5()
    run_workflow_6()
    run_workflow_7()
    run_workflow_8()
    print("\n=== Pipeline Complete ===")


def run_from_stage(start: int) -> None:
    """Run pipeline from a specific workflow number onwards."""
    for n in range(start, 9):
        fn = WORKFLOW_MAP.get(str(n))
        if fn:
            fn()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Solar Lead Intelligence Pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--workflow", "-w", type=str, choices=WORKFLOW_MAP.keys(),
        help="Run a single workflow (1-8)",
    )
    group.add_argument(
        "--from-stage", "-f", type=int, metavar="N",
        help="Run pipeline from stage N to 8",
    )
    group.add_argument(
        "--all", "-a", action="store_true",
        help="Run the full pipeline (default)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.workflow:
        WORKFLOW_MAP[args.workflow]()
    elif args.from_stage:
        run_from_stage(args.from_stage)
    else:
        run_full_pipeline()


if __name__ == "__main__":
    main()
