import json
import sys

from run_regression_update import run, latest_invocation_id


def main():

    # Reset isolated test data
    run(["run-operation", "regression_reset_delete_insert"])

    # Positive test
    output = run([
        "run", "--select", "regression_customer_delete_insert"
    ])

    expected = "DELETE_INSERT DML COMMITTED | Deleted=1 | Inserted=2"

    if expected not in output:
        raise RuntimeError("DELETE_INSERT DML counts incorrect")

    run(["test", "--select", "regression_delete_insert_state"])

    print("PASS: DELETE_INSERT positive test")

    # Transaction-critical DQ rollback test. The source deliberately differs
    # from the committed positive state, so a failed rollback is observable.
    output = run([
        "run", "--select", "regression_customer_delete_insert",
        "--vars", json.dumps({
            "regression_case": "critical_failure",
            "regression_delete_insert_force_critical_failure": True
        })
    ], should_succeed=False)

    if "DELETE_INSERT_CRITICAL_DQ_FAILED" not in output:
        raise RuntimeError(
            "Expected DELETE_INSERT critical-DQ rollback failure missing"
        )

    if "DELETE_INSERT DML COMMITTED" in output:
        raise RuntimeError(
            "Unexpected DELETE_INSERT commit during critical-DQ failure"
        )

    critical_invocation = latest_invocation_id()
    run([
        "run-operation",
        "regression_verify_transaction_dq_failure",
        "--args",
        json.dumps({
            "invocation_id": critical_invocation,
            "model_name": "regression_customer_delete_insert",
            "target_identifier": "DIM_CUSTOMER_DELETE_INSERT_TEST",
            "check_name": "DELETE_INSERT_TRANSACTION_CRITICAL_DQ"
        })
    ])

    run(["test", "--select", "regression_delete_insert_state"])

    print("PASS: DELETE_INSERT transaction-critical DQ audit persisted after rollback")
    print("PASS: DELETE_INSERT transaction-critical DQ rolls back DELETE + INSERT")

    # Negative test: NULL customer name
    output = run([
        "run", "--select", "regression_customer_delete_insert",
        "--vars", '{"regression_case":"null_name"}'
    ], should_succeed=False)

    if "SOURCE_REQUIRED_NOT_NULL | FAIL" not in output:
        raise RuntimeError("Expected NOT NULL failure missing")

    if "DELETE_INSERT DML COMMITTED" in output:
        raise RuntimeError("Unexpected DML commit during negative test")

    failed_invocation = latest_invocation_id()

    # Verify execution-specific failure audit
    run([
        "run-operation",
        "regression_verify_not_null_failure",
        "--args",
        json.dumps({
            "invocation_id": failed_invocation,
            "model_name":
                "regression_customer_delete_insert",
            "target_identifier":
                "DIM_CUSTOMER_DELETE_INSERT_TEST"
        })
    ])

    # Verify target state unchanged after failure
    run(["test", "--select", "regression_delete_insert_state"])

    print("\nPASS: DELETE_INSERT NULL rejection")
    print("PASS: Target unchanged after failed load")
    print("PASS: Execution-specific failure audit")
    print("\nALL DELETE_INSERT REGRESSION TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nDELETE_INSERT REGRESSION FAILED: {exc}")
        sys.exit(1)