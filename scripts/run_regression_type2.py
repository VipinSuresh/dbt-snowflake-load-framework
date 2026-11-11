import json
import sys

from run_regression_update import run, latest_invocation_id


def verify_dml(output, inserted, expired):
    expected = (
        f"TYPE2 DML COMMITTED | inserted versions={inserted} "
        f"| expired versions={expired}"
    )

    if expected not in output:
        raise RuntimeError(
            f"TYPE2 DML counts incorrect. Expected: {expected}"
        )


def main():

    # Reset isolated history
    run(["run-operation", "regression_reset_type2"])

    # Test 1: Initial history
    output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", '{"regression_case":"initial"}'
    ])

    verify_dml(output, 3, 0)
    run(["test", "--select", "regression_type2_initial"])

    print("PASS: TYPE2 initial history")

    # Validation failure after successful UPDATE + INSERT must undo BOTH.
    # The initial 3-row history remains unchanged despite a changed source.
    failure_output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", json.dumps({
            "regression_case": "changed",
            "regression_type2_force_critical_failure": True
        })
    ], should_succeed=False)
    if "TYPE2_CRITICAL_DQ_FAILED" not in failure_output:
        raise RuntimeError("Expected transaction-critical DQ failure missing")
    if "TYPE2 DML COMMITTED" in failure_output:
        raise RuntimeError("Failure-injected TYPE2 transaction was committed")

    critical_invocation = latest_invocation_id()
    run([
        "run-operation",
        "regression_verify_transaction_dq_failure",
        "--args",
        json.dumps({
            "invocation_id": critical_invocation,
            "model_name": "regression_customer_type2",
            "target_identifier": "DIM_CUSTOMER_TYPE2_TEST",
            "check_name": "TYPE2_TRANSACTION_CRITICAL_DQ"
        })
    ])

    run(["test", "--select", "regression_type2_initial"])
    print("PASS: TYPE2 transaction-critical DQ audit persisted after rollback")
    print("PASS: TYPE2 transaction-critical DQ rolls back UPDATE + INSERT")

    # Test 2: Changed source creates a new version
    output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", '{"regression_case":"changed"}'
    ])

    verify_dml(output, 1, 1)
    run(["test", "--select", "regression_type2_changed"])

    print("PASS: TYPE2 version change")

    # Test 3: Identical source must not create versions
    output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", '{"regression_case":"changed"}'
    ])

    verify_dml(output, 0, 0)
    run(["test", "--select", "regression_type2_changed"])

    print("PASS: TYPE2 unchanged rerun")

    # Test 4: Invalid source must be rejected
    output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", '{"regression_case":"null_name"}'
    ], should_succeed=False)

    if "SOURCE_REQUIRED_NOT_NULL | FAIL" not in output:
        raise RuntimeError("Expected NOT NULL failure missing")

    if "TYPE2 DML COMMITTED" in output:
        raise RuntimeError("Unexpected TYPE2 DML during failure")

    failed_invocation = latest_invocation_id()

    # Verify audit for exact failed execution
    run([
        "run-operation",
        "regression_verify_not_null_failure",
        "--args",
        json.dumps({
            "invocation_id": failed_invocation,
            "model_name":
                "regression_customer_type2",
            "target_identifier": "DIM_CUSTOMER_TYPE2_TEST"
        })
    ])

    # Historical target must remain unchanged
    run(["test", "--select", "regression_type2_changed"])

    print("PASS: TYPE2 NULL rejection")
    print("PASS: TYPE2 execution-specific failure audit")
    print("PASS: TYPE2 history unchanged after failure")
    print("\nALL TYPE2 REGRESSION TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTYPE2 REGRESSION FAILED: {exc}")
        sys.exit(1)