# Workflow 1: Lead Generation
# Builds search tasks from keywords × locations and saves to search_tasks.json.

import json
import itertools
from config.settings import SEARCH_TASKS_FILE
from .keyword_generator import generate_keywords, generate_locations


def build_search_tasks() -> list[dict]:
    """Cross-join keywords and locations into search task objects."""
    tasks = []
    for keyword, location in itertools.product(generate_keywords(), generate_locations()):
        tasks.append({
            "keyword": keyword,
            "location": location,
            "industry": "solar",
            "query": f"{keyword} {location}",
            "status": "pending",
        })
    return tasks


def save_search_tasks(tasks: list[dict]) -> None:
    """Persist search tasks to JSON file."""
    SEARCH_TASKS_FILE.write_text(json.dumps(tasks, indent=2), encoding="utf-8")
    print(f"[Workflow 1] Saved {len(tasks)} search tasks → {SEARCH_TASKS_FILE}")


def run() -> list[dict]:
    tasks = build_search_tasks()
    save_search_tasks(tasks)
    return tasks


if __name__ == "__main__":
    run()
