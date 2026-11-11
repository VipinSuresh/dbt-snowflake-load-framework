import subprocess
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]

SUITES = [
    "run_regression_type1.py",
    "run_regression_type2.py",
    "run_regression_insert.py",
    "run_regression_update.py",
    "run_regression_delete_insert.py",
    "run_regression_replace.py",
]

def main():
    for suite in SUITES:
        print(f"\n{'=' * 60}")
        print(f"RUNNING: {suite}")
        print(f"{'=' * 60}", flush=True)

        result = subprocess.run(
            [sys.executable, str(PROJECT / "scripts" / suite)],
            cwd=PROJECT,
        )

        if result.returncode != 0:
            print(f"\nFAILED: {suite}")
            sys.exit(1)

        print(f"\nPASSED: {suite}", flush=True)

    print("\nALL SIX ENTERPRISE LOAD REGRESSION SUITES PASSED")


if __name__ == "__main__":
    main()