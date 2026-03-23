"""
Solar Lead Intelligence — Entry Point

Usage:
    python main.py                  # Run the full 8-step pipeline
    python main.py --workflow 1     # Run only Workflow 1 (Lead Generation)
    python main.py --from-stage 5   # Run Workflows 5 → 8
    python main.py --help           # Show all options
"""

from src.pipeline.orchestrator import main

if __name__ == "__main__":
    main()
