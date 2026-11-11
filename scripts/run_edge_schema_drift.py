import json
import sys

from run_regression_update import run, latest_invocation_id


def verify_audit(invocation_id, model_name, target_identifier, check_name):
    run([
        "run-operation",
        "regression_verify_schema_drift_failure",
        "--args",
        json.dumps({
            "invocation_id": invocation_id,
            "model_name": model_name,
            "target_identifier": target_identifier,
            "check_name": check_name,
        }),
    ])


def run_expected_failure(model_name, regression_case, target_identifier, check_name, dml_marker, extra_expected=None, extra_expected_any=None):
    output = run([
        "run", "--select", model_name,
        "--vars", json.dumps({"regression_case": regression_case}),
    ], should_succeed=False)

    expected = f"{check_name} | FAIL"
    if expected not in output:
        raise RuntimeError(f"Expected pre-load failure missing: {expected}")

    if extra_expected and extra_expected not in output:
        raise RuntimeError(f"Expected diagnostic missing: {extra_expected}")

    if extra_expected_any and not any(value in output for value in extra_expected_any):
        raise RuntimeError(
            "Expected one of these diagnostics: " + " | ".join(extra_expected_any)
        )

    if dml_marker in output:
        raise RuntimeError(f"Unexpected business DML after schema-contract failure: {dml_marker}")

    invocation_id = latest_invocation_id()
    verify_audit(invocation_id, model_name, target_identifier, check_name)


def verify_type1_unchanged():
    run(["run-operation", "regression_verify_schema_drift_type1_baseline"])


def prepare_type2():
    run(["run-operation", "regression_reset_type2"])
    output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", '{"regression_case":"initial"}',
    ])
    expected = "TYPE2 DML COMMITTED | inserted versions=3 | expired versions=0"
    if expected not in output:
        raise RuntimeError("Unable to establish clean TYPE2 history fixture")
    run(["test", "--select", "regression_type2_initial"])


def test_generic_missing_column():
    print("\n" + "=" * 68)
    print("SCHEMA DRIFT: GENERIC SOURCE MISSING COLUMN")
    print("=" * 68)
    run(["run-operation", "regression_reset_type1"])
    run_expected_failure(
        "regression_customer_type1",
        "schema_missing_column",
        "DIM_CUSTOMER_TYPE1_TEST",
        "COLUMN_CONTRACT",
        "MERGE COMPLETE",
    )
    verify_type1_unchanged()
    print("PASS: Missing business column is blocked before DML")


def test_generic_extra_column():
    print("\n" + "=" * 68)
    print("SCHEMA DRIFT: GENERIC SOURCE EXTRA COLUMN")
    print("=" * 68)
    run(["run-operation", "regression_reset_type1"])
    run_expected_failure(
        "regression_customer_type1",
        "schema_extra_column",
        "DIM_CUSTOMER_TYPE1_TEST",
        "COLUMN_CONTRACT",
        "MERGE COMPLETE",
    )
    verify_type1_unchanged()
    print("PASS: Unexpected business column is blocked before DML")


def test_generic_type_mismatch():
    print("\n" + "=" * 68)
    print("SCHEMA DRIFT: GENERIC SOURCE DATATYPE MISMATCH")
    print("=" * 68)
    run(["run-operation", "regression_reset_type1"])
    run_expected_failure(
        "regression_customer_type1",
        "schema_type_mismatch",
        "DIM_CUSTOMER_TYPE1_TEST",
        "TYPE_CONTRACT",
        "MERGE COMPLETE",
        extra_expected_any=[
            "CUSTOMER_ID:VARCHAR->NUMBER",
            "CUSTOMER_ID:CHARACTER VARYING->NUMBER",
        ],
    )
    verify_type1_unchanged()
    print("PASS: Incompatible source datatype is blocked before DML")


def test_type2_missing_column():
    print("\n" + "=" * 68)
    print("SCHEMA DRIFT: TYPE2 SOURCE MISSING COLUMN")
    print("=" * 68)
    prepare_type2()
    run_expected_failure(
        "regression_customer_type2",
        "schema_missing_column",
        "DIM_CUSTOMER_TYPE2_TEST",
        "TYPE2_COLUMN_CONTRACT",
        "TYPE2 DML COMMITTED",
    )
    run(["test", "--select", "regression_type2_initial"])
    print("PASS: TYPE2 missing business column is blocked before DML")


def test_type2_type_mismatch():
    print("\n" + "=" * 68)
    print("SCHEMA DRIFT: TYPE2 SOURCE DATATYPE MISMATCH")
    print("=" * 68)
    prepare_type2()
    run_expected_failure(
        "regression_customer_type2",
        "schema_type_mismatch",
        "DIM_CUSTOMER_TYPE2_TEST",
        "TYPE2_TYPE_CONTRACT",
        "TYPE2 DML COMMITTED",
        extra_expected_any=[
            "CUSTOMER_ID:VARCHAR->NUMBER",
            "CUSTOMER_ID:CHARACTER VARYING->NUMBER",
        ],
    )
    run(["test", "--select", "regression_type2_initial"])
    print("PASS: TYPE2 incompatible source datatype is blocked before DML")


def test_type2_reserved_column():
    print("\n" + "=" * 68)
    print("SCHEMA DRIFT: TYPE2 RESERVED FRAMEWORK COLUMN IN SOURCE")
    print("=" * 68)
    prepare_type2()
    run_expected_failure(
        "regression_customer_type2",
        "schema_reserved_column",
        "DIM_CUSTOMER_TYPE2_TEST",
        "TYPE2_COLUMN_CONTRACT",
        "TYPE2 DML COMMITTED",
        "reserved source columns=EL_ACTION",
    )
    run(["test", "--select", "regression_type2_initial"])
    print("PASS: TYPE2 reserved framework metadata in source is blocked before DML")


def restore_clean_fixtures():
    print("\nRestoring clean regression fixtures...")
    run(["run-operation", "regression_reset_type1"])
    prepare_type2()
    print("REGRESSION TARGETS RESTORED TO CLEAN STATE")


def main():
    print("SCHEMA-DRIFT PRE-LOAD EDGE-CASE REGRESSION")
    print("Exercises existing COLUMN_CONTRACT / TYPE_CONTRACT checks; framework macros are not modified.")
    print("TYPE1 represents the shared validation path used by the five non-TYPE2 strategies.\n")

    try:
        test_generic_missing_column()
        test_generic_extra_column()
        test_generic_type_mismatch()
        test_type2_missing_column()
        test_type2_type_mismatch()
        test_type2_reserved_column()
    finally:
        restore_clean_fixtures()

    print("\nALL SIX SCHEMA-DRIFT PRE-LOAD EDGE-CASE TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nSCHEMA-DRIFT EDGE-CASE REGRESSION FAILED: {exc}")
        sys.exit(1)
