#!/usr/bin/env python3

import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

TARGET_A = "DIM_CUSTOMER_REPLACE_TEST_A"
TARGET_B = "DIM_CUSTOMER_REPLACE_TEST_B"

TARGET_PATH_A = ROOT / "target_concurrency_truncate_distinct_A"
TARGET_PATH_B = ROOT / "target_concurrency_truncate_distinct_B"

LOG_PATH_A = ROOT / "logs_concurrency_truncate_distinct_A"
LOG_PATH_B = ROOT / "logs_concurrency_truncate_distinct_B"


def run_checked(args, label):
    print(f"\nRunning: {' '.join(args)}")
    result = subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    print(result.stdout)

    if result.returncode != 0:
        print(f"\nFAIL: {label}")
        sys.exit(result.returncode)

    return result.stdout


def build_run_command(target_alias, target_path, log_path):
    variables = {
        "regression_replace_target_alias": target_alias,
        "regression_truncate_concurrency_wait_seconds": 12,
    }

    return [
        "dbt",
        "run",
        "--select",
        "regression_customer_replace",
        "--vars",
        json.dumps(variables),
        "--no-partial-parse",
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
    ]


def build_state_test_command(target_alias):
    variables = {
        "regression_replace_target_alias": target_alias,
    }

    return [
        "dbt",
        "test",
        "--select",
        "regression_replace_state",
        "--vars",
        json.dumps(variables),
        "--no-partial-parse",
    ]


def main():
    print("TRUNCATE_LOAD DISTINCT-TARGET CONCURRENCY REGRESSION")
    print(
        "Goal: two independent full replacements target different physical "
        "tables while sharing the global TRUNCATE_LOAD serialization lock. "
        "Both executions must eventually succeed."
    )

    # Ensure the global framework serialization row exists.
    run_checked(
        [
            "dbt",
            "run-operation",
            "regression_prepare_truncate_load_lock",
        ],
        "prepare TRUNCATE_LOAD framework lock",
    )

    # Restore the canonical deterministic 3-row baseline.
    run_checked(
        [
            "dbt",
            "run-operation",
            "regression_reset_replace",
        ],
        "reset canonical TRUNCATE_LOAD baseline",
    )

    # Clone the canonical baseline into two independent physical targets.
    run_checked(
        [
            "dbt",
            "run-operation",
            "regression_prepare_replace_distinct_targets",
        ],
        "prepare distinct TRUNCATE_LOAD targets",
    )

    cmd_a = build_run_command(
        TARGET_A,
        TARGET_PATH_A,
        LOG_PATH_A,
    )

    cmd_b = build_run_command(
        TARGET_B,
        TARGET_PATH_B,
        LOG_PATH_B,
    )

    print(
        f"\nStarting concurrent TRUNCATE_LOAD run A -> {TARGET_A}: "
        f"{' '.join(cmd_a)}"
    )

    print(
        f"\nStarting concurrent TRUNCATE_LOAD run B -> {TARGET_B}: "
        f"{' '.join(cmd_b)}"
    )

    proc_a = subprocess.Popen(
        cmd_a,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    proc_b = subprocess.Popen(
        cmd_b,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Drain both pipes concurrently.
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(proc_a.communicate)
        future_b = executor.submit(proc_b.communicate)

        out_a = future_a.result()[0]
        out_b = future_b.result()[0]

    print(
        f"\n===== DISTINCT-TARGET TRUNCATE_LOAD RUN A "
        f"| exit={proc_a.returncode} ====="
    )
    print(out_a)

    print(
        f"\n===== DISTINCT-TARGET TRUNCATE_LOAD RUN B "
        f"| exit={proc_b.returncode} ====="
    )
    print(out_b)

    if proc_a.returncode != 0 or proc_b.returncode != 0:
        print(
            "\nFAIL: both different-target TRUNCATE_LOAD executions "
            "must succeed."
        )
        sys.exit(1)

    combined_output = out_a + "\n" + out_b

    if "TARGET_CHANGED_SINCE_PRECHECK" in combined_output:
        print(
            "\nFAIL: different-target execution incorrectly raised "
            "TARGET_CHANGED_SINCE_PRECHECK."
        )
        sys.exit(1)

    print(
        "\nPASS: both different-target TRUNCATE_LOAD executions succeeded"
    )

    # Reuse the existing final-state regression test against target A.
    run_checked(
        build_state_test_command(TARGET_A),
        "validate distinct target A",
    )

    print(f"\nPASS: {TARGET_A} final replacement snapshot verified")

    # Reuse the same final-state test against target B.
    run_checked(
        build_state_test_command(TARGET_B),
        "validate distinct target B",
    )

    print(f"\nPASS: {TARGET_B} final replacement snapshot verified")

    print(
        "\nTRUNCATE_LOAD DISTINCT-TARGET CONCURRENCY REGRESSION PASSED"
    )


if __name__ == "__main__":
    main()
