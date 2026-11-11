#!/usr/bin/env python3
import json
import subprocess
import sys

def run(cmd, expect_success=True):
    print("\nRunning:", " ".join(cmd), flush=True)
    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(p.stdout, flush=True)
    if expect_success and p.returncode != 0:
        raise RuntimeError(f"Unexpected dbt exit code: {p.returncode}")
    if not expect_success and p.returncode == 0:
        raise RuntimeError("Expected dbt command to fail, but it succeeded")
    return p.stdout, p.returncode

def dbt(*args):
    return ["dbt", *args]

def run_expected_runtime_failure(model, vars_dict, forbidden_custom_exception):
    output, _ = run(
        dbt("run", "--select", model, "--vars", json.dumps(vars_dict)),
        expect_success=False,
    )

    if "RUNTIME_FAULT_SENTINEL" not in output:
        raise RuntimeError(
            f"{model}: expected injected Snowflake runtime-failure sentinel was not present"
        )

    if forbidden_custom_exception in output:
        raise RuntimeError(
            f"{model}: got controlled transaction-DQ exception instead of unexpected SQL runtime error"
        )

    committed_markers = [
        "TYPE2 DML COMMITTED",
        "DELETE_INSERT DML COMMITTED",
        "TRUNCATE_LOAD DML COMMITTED",
    ]
    if any(marker in output for marker in committed_markers):
        raise RuntimeError(
            f"{model}: transaction reported committed DML despite injected runtime failure"
        )

    return output

def verify_audit(model_name, target_identifier):
    run(dbt(
        "run-operation",
        "regression_verify_runtime_failure_audit",
        "--args",
        json.dumps({
            "model_name": model_name,
            "target_identifier": target_identifier,
        }),
    ))

def main():
    print("UNEXPECTED SQL/RUNTIME FAILURE REGRESSION")
    print("Forces a real Snowflake expression error after the first DML inside each transaction.")
    print("Expected: transaction rollback + unchanged target + conservative failed audit.\n")

    # TYPE2: initialize history, then expire a row and fail before inserting new version.
    print("=" * 68)
    print("RUNTIME FAILURE: TYPE2")
    print("=" * 68)
    run(dbt("run-operation", "regression_reset_type2"))
    run(dbt("run", "--select", "regression_customer_type2",
            "--vars", json.dumps({"regression_case": "initial"})))
    run(dbt("test", "--select", "regression_type2_initial"))

    run_expected_runtime_failure(
        "regression_customer_type2",
        {
            "regression_case": "changed",
            "regression_type2_force_runtime_failure": True,
        },
        "TYPE2_CRITICAL_DQ_FAILED",
    )
    verify_audit("regression_customer_type2", "DIM_CUSTOMER_TYPE2_TEST")
    run(dbt("test", "--select", "regression_type2_initial"))
    print("PASS: TYPE2 unexpected SQL error rolled back expired-history DML")

    # DELETE_INSERT: establish the normal post-load state, then delete matching keys
    # and fail before re-inserting them.
    print("\n" + "=" * 68)
    print("RUNTIME FAILURE: DELETE_INSERT")
    print("=" * 68)
    run(dbt("run-operation", "regression_reset_delete_insert"))
    run(dbt("run", "--select", "regression_customer_delete_insert"))
    run(dbt("test", "--select", "regression_delete_insert_state"))

    run_expected_runtime_failure(
        "regression_customer_delete_insert",
        {"regression_delete_insert_force_runtime_failure": True},
        "DELETE_INSERT_CRITICAL_DQ_FAILED",
    )
    verify_audit(
        "regression_customer_delete_insert",
        "DIM_CUSTOMER_DELETE_INSERT_TEST",
    )
    run(dbt("test", "--select", "regression_delete_insert_state"))
    print("PASS: DELETE_INSERT unexpected SQL error rolled back DELETE")

    # TRUNCATE_LOAD: establish a valid replacement state, then delete the entire
    # target and fail before replacement INSERT. Rollback must restore snapshot.
    print("\n" + "=" * 68)
    print("RUNTIME FAILURE: TRUNCATE_LOAD")
    print("=" * 68)
    run(dbt("run-operation", "regression_reset_replace"))
    run(dbt("run", "--select", "regression_customer_replace"))
    run(dbt("test", "--select", "regression_replace_state"))

    run_expected_runtime_failure(
        "regression_customer_replace",
        {"regression_truncate_force_runtime_failure": True},
        "TRUNCATE_LOAD_CRITICAL_DQ_FAILED",
    )
    verify_audit("regression_customer_replace", "DIM_CUSTOMER_REPLACE_TEST")
    run(dbt("test", "--select", "regression_replace_state"))
    print("PASS: TRUNCATE_LOAD unexpected SQL error rolled back full-target DELETE")

    print("\nALL THREE UNEXPECTED SQL/RUNTIME FAILURE TESTS PASSED")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nUNEXPECTED SQL/RUNTIME FAILURE REGRESSION FAILED: {exc}", flush=True)
        sys.exit(1)
