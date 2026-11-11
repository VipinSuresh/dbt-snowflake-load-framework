import json
import sys

from run_regression_update import run, latest_invocation_id


def main():

    # Reset isolated test target
    run(["run-operation", "regression_reset_replace"])

    # Positive test: full replacement
    output = run([
        "run", "--select", "regression_customer_replace"
    ])

    expected = (
        "TRUNCATE_LOAD DML COMMITTED | Deleted=3 | Inserted=2"
    )

    if expected not in output:
        raise RuntimeError(
            "TRUNCATE_LOAD DML counts incorrect"
        )

    run(["test", "--select", "regression_replace_state"])

    print("PASS: TRUNCATE_LOAD positive test")

    # Transaction-critical DQ rollback test. The source deliberately differs
    # from the committed positive state, so a failed rollback is observable.
    output = run([
        "run", "--select", "regression_customer_replace",
        "--vars", json.dumps({
            "regression_case": "critical_failure",
            "regression_truncate_force_critical_failure": True
        })
    ], should_succeed=False)

    if "TRUNCATE_LOAD_CRITICAL_DQ_FAILED" not in output:
        raise RuntimeError(
            "Expected TRUNCATE_LOAD critical-DQ rollback failure missing"
        )

    if "TRUNCATE_LOAD DML COMMITTED" in output:
        raise RuntimeError(
            "Unexpected TRUNCATE_LOAD commit during critical-DQ failure"
        )

    critical_invocation = latest_invocation_id()
    run([
        "run-operation",
        "regression_verify_transaction_dq_failure",
        "--args",
        json.dumps({
            "invocation_id": critical_invocation,
            "model_name": "regression_customer_replace",
            "target_identifier": "DIM_CUSTOMER_REPLACE_TEST",
            "check_name": "TRUNCATE_LOAD_TRANSACTION_CRITICAL_DQ"
        })
    ])

    run(["test", "--select", "regression_replace_state"])

    print("PASS: TRUNCATE_LOAD transaction-critical DQ audit persisted after rollback")
    print("PASS: TRUNCATE_LOAD transaction-critical DQ rolls back DELETE + INSERT")

    # Negative test: NULL customer name
    output = run([
        "run", "--select", "regression_customer_replace",
        "--vars", '{"regression_case":"null_name"}'
    ], should_succeed=False)

    if "SOURCE_REQUIRED_NOT_NULL | FAIL" not in output:
        raise RuntimeError(
            "Expected NOT NULL failure missing"
        )

    if "TRUNCATE_LOAD DML COMMITTED" in output:
        raise RuntimeError(
            "Unexpected DML commit during negative test"
        )

    failed_invocation = latest_invocation_id()

    # Verify audit for exact failed execution
    run([
        "run-operation",
        "regression_verify_not_null_failure",
        "--args",
        json.dumps({
            "invocation_id": failed_invocation,
            "model_name":
                "regression_customer_replace",
            "target_identifier":
                "DIM_CUSTOMER_REPLACE_TEST"
        })
    ])

    # Verify expected target state still holds
    run(["test", "--select", "regression_replace_state"])

    print("\nPASS: TRUNCATE_LOAD NULL rejection")
    print("PASS: Expected target state preserved")
    print("PASS: Execution-specific failure audit")

    print("\nALL TRUNCATE_LOAD REGRESSION TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTRUNCATE_LOAD REGRESSION FAILED: {exc}")
        sys.exit(1)