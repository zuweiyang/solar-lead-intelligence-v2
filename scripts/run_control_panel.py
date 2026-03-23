"""
Workflow 9.5 — Launch Script for the Streamlit Campaign Control Panel.

Usage:
    py scripts/run_control_panel.py

This script prints the correct launch command and optionally opens it.
"""
import subprocess
import sys
from pathlib import Path

APP_PATH = Path(__file__).parent.parent / "src" / "workflow_9_5_streamlit_control_panel" / "app.py"


def main() -> None:
    # Prefer `py -m streamlit` so the script works even when the streamlit
    # executable is not on PATH (common on Windows with user-level installs).
    cmd = [sys.executable, "-m", "streamlit", "run", str(APP_PATH)]
    print("Launching Campaign Control Panel...")
    print(f"  Command: {' '.join(cmd)}")
    print()
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\nControl panel stopped.")


if __name__ == "__main__":
    main()
