import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from run_regression_update import PROJECT, run


WAIT_SECONDS = 12
START_STAGGER_SECONDS = 0.75
PROCESS_TIMEOUT_SECONDS = 240


def _concurrent_command(label):
    target_path = PROJECT / f"target_concurrency_{label}"
    log_path = PROJECT / f"logs_concurrency_{label}"

    shutil.rmtree(target_path, ignore_errors=True)
    shutil.rmtree(log_path, ignore_errors=True)

    vars_payload = {
        "regression_case": "changed",
        "regression_type2_concurrency_wait_seconds": WAIT_SECONDS,
    }

    cmd = [
        "dbt",
        "run",
        "--select",
        "regression_customer_type2",
        "--vars",
        json.dumps(vars_payload),
        "--no-partial-parse",
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
    ]
    return cmd, target_path


def _start(label):
    cmd, target_path = _concurrent_command(label)
    print(f"\nStarting concurrent TYPE2 run {label}: {' '.join(cmd)}", flush=True)
    proc = subprocess.Popen(
        cmd,
        cwd=PROJECT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc, target_path


def _finish(label, proc):
    try:
        output, _ = proc.communicate(timeout=PROCESS_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        output, _ = proc.communicate()
        print(output, flush=True)
        raise RuntimeError(f"Concurrent TYPE2 run {label} timed out")

    print(f"\n===== CONCURRENT TYPE2 RUN {label} | exit={proc.returncode} =====", flush=True)
    print(output, flush=True)
    return proc.returncode, output


def _invocation_id(target_path):
    results_path = target_path / "run_results.json"
    if not results_path.exists():
        raise RuntimeError(f"Missing dbt artifact: {results_path}")

    data = json.loads(results_path.read_text())
    invocation_id = data.get("metadata", {}).get("invocation_id")
    if not invocation_id:
        raise RuntimeError(f"Missing invocation_id in {results_path}")
    return invocation_id


def _verify_failed_audit(invocation_id):
    run([
        "run-operation",
        "regression_verify_transaction_dq_failure",
        "--args",
        json.dumps({
            "invocation_id": invocation_id,
            "model_name": "regression_customer_type2",
            "target_identifier": "DIM_CUSTOMER_TYPE2_TEST",
            "check_name": "TYPE2_TRANSACTION_CRITICAL_DQ",
            "expected_reason": "AFFECTED_KEY_DUPLICATE_VERSION",
        }),
    ])


def main():
    print("TYPE2 CONCURRENCY REGRESSION", flush=True)
    print(
        "Goal: overlap two independent dbt/Snowflake sessions after TYPE2 UPDATE "
        "has acquired the target write lock.",
        flush=True,
    )

    # Establish the exact three-row version-1 baseline.
    run(["run-operation", "regression_reset_type2"])
    baseline = run([
        "run",
        "--select",
        "regression_customer_type2",
        "--vars",
        '{"regression_case":"initial"}',
    ])
    if "TYPE2 DML COMMITTED | inserted versions=3 | expired versions=0" not in baseline:
        raise RuntimeError("TYPE2 concurrency baseline did not initialize correctly")
    run(["test", "--select", "regression_type2_initial"])
    print("PASS: TYPE2 concurrency baseline initialized", flush=True)

    # Use two separate dbt processes, therefore two separate Snowflake sessions.
    # Each process writes artifacts/logs to its own path so local dbt files do not race.
    proc_a, target_a = _start("a")
    time.sleep(START_STAGGER_SECONDS)
    proc_b, target_b = _start("b")

    rc_a, out_a = _finish("A", proc_a)
    rc_b, out_b = _finish("B", proc_b)

    outcomes = [
        ("A", rc_a, out_a, target_a),
        ("B", rc_b, out_b, target_b),
    ]

    successes = [x for x in outcomes if x[1] == 0]
    failures = [x for x in outcomes if x[1] != 0]

    if not successes:
        raise RuntimeError("Both concurrent TYPE2 executions failed")

    for label, _, output, _ in outcomes:
        if "TYPE2 REGRESSION CONCURRENCY HOLD" not in output:
            raise RuntimeError(f"Run {label} did not enable the concurrency hold")

    for label, _, output, _ in successes:
        if "TYPE2 DML COMMITTED" not in output:
            raise RuntimeError(f"Run {label} returned success without TYPE2 commit evidence")

    for label, _, output, target_path in failures:
        if "TYPE2_CRITICAL_DQ_FAILED" not in output:
            raise RuntimeError(
                f"Run {label} failed for an unexpected reason instead of the framework's "
                "transaction-critical DQ guard"
            )
        invocation_id = _invocation_id(target_path)
        _verify_failed_audit(invocation_id)
        print(f"PASS: Run {label} failed safely and persisted transaction-DQ audit", flush=True)

    # This is the decisive assertion. It checks exact row count, one current row per key,
    # one version 2 for customer 101, and continuous SCD2 validity windows.
    run(["test", "--select", "regression_type2_changed"])

    print("\nPASS: Concurrent TYPE2 final history is valid", flush=True)
    print(f"PASS: successful executions={len(successes)} failed executions={len(failures)}", flush=True)
    if failures:
        print("PASS: contention was rejected cleanly by transaction-critical DQ", flush=True)
    else:
        print("PASS: both executions serialized without corrupting TYPE2 history", flush=True)
    print("\nTYPE2 CONCURRENCY REGRESSION PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTYPE2 CONCURRENCY REGRESSION FAILED: {exc}", flush=True)
        sys.exit(1)
