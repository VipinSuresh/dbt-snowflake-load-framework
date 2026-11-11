import json
import sys

from run_regression_update import run, latest_invocation_id


def main():

    # Reset the isolated INSERT test table
    run(["run-operation", "regression_reset_insert"])

    # Positive test
    output = run([
        "run", "--select", "regression_customer_insert"
    ])

    expected = "MERGE COMPLETE | Inserted=1 | Updated=0 | Deleted=0"

    if expected not in output:
        raise RuntimeError("INSERT DML counts incorrect")

    run(["test", "--select", "regression_insert_state"])

    print("PASS: INSERT positive test")

    # Negative test: NULL customer name
    output = run([
        "run", "--select", "regression_customer_insert",
        "--vars", '{"regression_case":"null_name"}'
    ], should_succeed=False)

    if "SOURCE_REQUIRED_NOT_NULL | FAIL" not in output:
        raise RuntimeError("Expected NOT NULL failure missing")

    if "MERGE COMPLETE" in output:
        raise RuntimeError("Unexpected DML during negative test")

    failed_invocation = latest_invocation_id()

    # Verify failure audit for this exact execution
    run([
        "run-operation", "regression_verify_insert_audit",
        "--args", json.dumps({
            "invocation_id": failed_invocation
        })
    ])

    # Verify no data changed after failure
    run(["test", "--select", "regression_insert_state"])

    print("\nPASS: INSERT negative NOT NULL test")
    print("PASS: Target unchanged after failure")
    print("PASS: Execution-specific audit")
    print("\nALL INSERT REGRESSION TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nINSERT REGRESSION FAILED: {exc}")
        sys.exit(1)