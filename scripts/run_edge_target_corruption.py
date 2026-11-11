import json
import sys

from run_regression_update import run, latest_invocation_id


def corrupt(case):
    run([
        "run-operation",
        "regression_corrupt_target_state",
        "--args",
        json.dumps({"corruption_case": case}),
    ])


def verify_audit(invocation_id, model_name, target_identifier, check_name):
    run([
        "run-operation",
        "regression_verify_preload_target_failure",
        "--args",
        json.dumps({
            "invocation_id": invocation_id,
            "model_name": model_name,
            "target_identifier": target_identifier,
            "check_name": check_name,
        }),
    ])


def verify_state(case):
    run([
        "run-operation",
        "regression_verify_target_corruption_state",
        "--args",
        json.dumps({"corruption_case": case}),
    ])


def run_expected_preload_failure(model_name, target_identifier, check_name, dml_marker):
    output = run([
        "run", "--select", model_name,
    ], should_succeed=False)

    expected = f"{check_name} | FAIL"
    if expected not in output:
        raise RuntimeError(f"Expected pre-load failure missing: {expected}")

    if dml_marker in output:
        raise RuntimeError(
            f"Unexpected business DML after target corruption: {dml_marker}"
        )

    invocation_id = latest_invocation_id()
    verify_audit(invocation_id, model_name, target_identifier, check_name)


def prepare_type2():
    run(["run-operation", "regression_reset_type2"])
    output = run([
        "run", "--select", "regression_customer_type2",
        "--vars", '{"regression_case":"initial"}',
    ])
    if "TYPE2 DML COMMITTED | inserted versions=3 | expired versions=0" not in output:
        raise RuntimeError("Unable to establish clean TYPE2 history fixture")
    run(["test", "--select", "regression_type2_initial"])


def test_type1_duplicate_key():
    print("\n" + "=" * 68)
    print("TARGET CORRUPTION: GENERIC DUPLICATE NATURAL KEY")
    print("=" * 68)
    run(["run-operation", "regression_reset_type1"])
    corrupt("type1_duplicate_key")
    run_expected_preload_failure(
        "regression_customer_type1",
        "DIM_CUSTOMER_TYPE1_TEST",
        "TARGET_DUPLICATE_KEYS",
        "MERGE COMPLETE",
    )
    verify_state("type1_duplicate_key")
    print("PASS: Existing duplicate target key blocks the load and remains untouched")


def test_type2_duplicate_current():
    print("\n" + "=" * 68)
    print("TARGET CORRUPTION: TYPE2 DUPLICATE CURRENT")
    print("=" * 68)
    prepare_type2()
    corrupt("type2_duplicate_current")
    run_expected_preload_failure(
        "regression_customer_type2",
        "DIM_CUSTOMER_TYPE2_TEST",
        "TYPE2_DUPLICATE_CURRENT",
        "TYPE2 DML COMMITTED",
    )
    verify_state("type2_duplicate_current")
    print("PASS: TYPE2 multiple-current corruption is detected before DML")


def test_type2_duplicate_version():
    print("\n" + "=" * 68)
    print("TARGET CORRUPTION: TYPE2 DUPLICATE VERSION NUMBER")
    print("=" * 68)
    prepare_type2()
    corrupt("type2_duplicate_version")
    run_expected_preload_failure(
        "regression_customer_type2",
        "DIM_CUSTOMER_TYPE2_TEST",
        "TYPE2_DUPLICATE_VERSION_NUMBER",
        "TYPE2 DML COMMITTED",
    )
    verify_state("type2_duplicate_version")
    print("PASS: TYPE2 duplicate VERSION_NO corruption is detected before DML")


def test_type2_history_overlap():
    print("\n" + "=" * 68)
    print("TARGET CORRUPTION: TYPE2 OVERLAPPING HISTORY WINDOWS")
    print("=" * 68)
    prepare_type2()
    corrupt("type2_history_overlap")
    run_expected_preload_failure(
        "regression_customer_type2",
        "DIM_CUSTOMER_TYPE2_TEST",
        "TYPE2_HISTORY_OVERLAP",
        "TYPE2 DML COMMITTED",
    )
    verify_state("type2_history_overlap")
    print("PASS: TYPE2 overlapping history is detected before DML")


def restore_clean_fixtures():
    print("\nRestoring clean regression fixtures...")
    run(["run-operation", "regression_reset_type1"])
    prepare_type2()
    print("REGRESSION TARGETS RESTORED TO CLEAN STATE")


def main():
    print("PRE-CORRUPTED TARGET EDGE-CASE REGRESSION")
    print("Tests corrupt target states Snowflake can physically store; framework macros are not modified.")
    print("TARGET_NULL_KEYS is intentionally excluded because CUSTOMER_ID is NOT NULL in the regression target.\n")

    try:
        test_type1_duplicate_key()
        test_type2_duplicate_current()
        test_type2_duplicate_version()
        test_type2_history_overlap()
    finally:
        restore_clean_fixtures()

    print("\nALL FOUR PRE-CORRUPTED TARGET EDGE-CASE TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTARGET CORRUPTION EDGE-CASE REGRESSION FAILED: {exc}")
        sys.exit(1)
