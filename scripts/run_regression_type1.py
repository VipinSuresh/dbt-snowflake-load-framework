import json
import sys

from run_regression_update import run, latest_invocation_id


def main():

    # Reset test data
    run(["run-operation", "regression_reset_type1"])

    # Positive TYPE1 test
    output = run([
        "run", "--select", "regression_customer_type1"
    ])

    expected = "MERGE COMPLETE | Inserted=1 | Updated=1 | Deleted=0"

    if expected not in output:
        raise RuntimeError("TYPE1 DML counts incorrect")

    run(["test", "--select", "regression_type1_state"])

    print("PASS: TYPE1 insert/update test")

    # Negative test
    output = run([
        "run", "--select", "regression_customer_type1",
        "--vars", '{"regression_case":"null_name"}'
    ], should_succeed=False)

    if "SOURCE_REQUIRED_NOT_NULL | FAIL" not in output:
        raise RuntimeError("Expected NOT NULL failure missing")

    if "MERGE COMPLETE" in output:
        raise RuntimeError("DML executed during negative test")

    failed_invocation = latest_invocation_id()

    # Verify execution-specific failure audit
    run([
        "run-operation", "regression_verify_type1_audit",
        "--args", json.dumps({
            "invocation_id": failed_invocation
        })
    ])

    # Confirm target unchanged
    run(["test", "--select", "regression_type1_state"])

    print("\nPASS: TYPE1 negative NOT NULL test")
    print("PASS: Target unchanged after failure")
    print("PASS: Execution-specific audit")
    print("\nALL TYPE1 REGRESSION TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTYPE1 REGRESSION FAILED: {exc}")
        sys.exit(1)