import json
import subprocess
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]


def run(args, should_succeed=True):
    print("\nRunning:", " ".join(args), flush=True)

    result = subprocess.run(
        ["dbt", *args, "--no-partial-parse"],
        cwd=PROJECT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    print(result.stdout, flush=True)

    if (result.returncode == 0) != should_succeed:
        raise RuntimeError(
            f"Unexpected dbt exit code: {result.returncode}"
        )

    return result.stdout


def latest_invocation_id():
    path = PROJECT / "target" / "run_results.json"
    data = json.loads(path.read_text())

    invocation_id = data.get("metadata", {}).get("invocation_id")

    if not invocation_id:
        raise RuntimeError("Missing dbt invocation ID")

    return invocation_id


def main():
    # Reset regression fixture
    run(["run-operation", "regression_reset_update"])

    # Positive test
    output = run([
        "run", "--select", "regression_customer_update"
    ])

    expected = "MERGE COMPLETE | Inserted=0 | Updated=1 | Deleted=0"

    if expected not in output:
        raise RuntimeError("Expected exactly one UPDATE")

    run(["test", "--select", "regression_update_state"])
    print("PASS: Positive UPDATE test")

    # Negative test
    output = run([
        "run", "--select", "regression_customer_update",
        "--vars", '{"regression_case":"null_name"}'
    ], should_succeed=False)

    if "SOURCE_REQUIRED_NOT_NULL | FAIL" not in output:
        raise RuntimeError("Expected NOT NULL failure missing")

    if "MERGE COMPLETE" in output:
        raise RuntimeError("Unexpected DML during negative test")

    # Capture failed dbt invocation before another command
    failed_invocation = latest_invocation_id()

    # Verify audit for this exact execution
    run([
        "run-operation", "regression_verify_null_failure",
        "--args", json.dumps({
            "invocation_id": failed_invocation
        })
    ])

    # Verify target unchanged
    run(["test", "--select", "regression_update_state"])

    print("\nPASS: Negative NOT NULL test")
    print("PASS: Execution-specific failure audit")
    print("PASS: Target unchanged")
    print("\nALL UPDATE REGRESSION TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nREGRESSION FAILED: {exc}")
        sys.exit(1)