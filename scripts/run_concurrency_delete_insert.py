import json
import shutil
import subprocess
import sys
import time

from run_regression_update import PROJECT, run


WAIT_SECONDS = 12
START_STAGGER_SECONDS = 0.75
PROCESS_TIMEOUT_SECONDS = 240


def _concurrent_command(label):
    target_path = PROJECT / f"target_concurrency_delete_insert_{label}"
    log_path = PROJECT / f"logs_concurrency_delete_insert_{label}"

    shutil.rmtree(target_path, ignore_errors=True)
    shutil.rmtree(log_path, ignore_errors=True)

    vars_payload = {
        "regression_delete_insert_concurrency_wait_seconds": WAIT_SECONDS,
    }

    cmd = [
        "dbt",
        "run",
        "--select",
        "regression_customer_delete_insert",
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
    print(
        f"\nStarting concurrent DELETE_INSERT run {label}: {' '.join(cmd)}",
        flush=True,
    )
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
        raise RuntimeError(f"Concurrent DELETE_INSERT run {label} timed out")

    print(
        f"\n===== CONCURRENT DELETE_INSERT RUN {label} | exit={proc.returncode} =====",
        flush=True,
    )
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
            "model_name": "regression_customer_delete_insert",
            "target_identifier": "DIM_CUSTOMER_DELETE_INSERT_TEST",
            "check_name": "DELETE_INSERT_TRANSACTION_CRITICAL_DQ",
            "expected_reason": "DML_COUNT_MISMATCH",
        }),
    ])


def main():
    print("DELETE_INSERT CONCURRENCY REGRESSION", flush=True)
    print(
        "Goal: overlap two independent dbt/Snowflake sessions using the same "
        "DELETE_INSERT source so stale pre-transaction overlap counts cannot commit.",
        flush=True,
    )

    # Deterministic 3-row baseline: 101/102/103.
    run(["run-operation", "regression_reset_delete_insert"])
    print("PASS: DELETE_INSERT concurrency baseline initialized", flush=True)

    proc_a, target_a = _start("A")
    time.sleep(START_STAGGER_SECONDS)
    proc_b, target_b = _start("B")

    rc_a, out_a = _finish("A", proc_a)
    rc_b, out_b = _finish("B", proc_b)

    outcomes = [
        ("A", rc_a, out_a, target_a),
        ("B", rc_b, out_b, target_b),
    ]

    for label, _, output, _ in outcomes:
        if "DELETE_INSERT REGRESSION CONCURRENCY HOLD" not in output:
            raise RuntimeError(f"Run {label} did not enable the concurrency hold")

    successes = [x for x in outcomes if x[1] == 0]
    failures = [x for x in outcomes if x[1] != 0]

    if not successes:
        raise RuntimeError("Both concurrent DELETE_INSERT executions failed")

    for label, _, output, _ in successes:
        if "DELETE_INSERT DML COMMITTED" not in output:
            raise RuntimeError(
                f"Run {label} returned success without DELETE_INSERT commit evidence"
            )

    for label, _, output, target_path in failures:
        if "DELETE_INSERT_CRITICAL_DQ_FAILED" not in output:
            raise RuntimeError(
                f"Run {label} failed for an unexpected reason instead of the "
                "transaction-critical DELETE_INSERT guard"
            )
        if "DELETE_INSERT DML COMMITTED" in output:
            raise RuntimeError(
                f"Run {label} logged a commit despite transaction-critical failure"
            )
        invocation_id = _invocation_id(target_path)
        _verify_failed_audit(invocation_id)
        print(
            f"PASS: Run {label} failed safely and persisted DML_COUNT_MISMATCH audit",
            flush=True,
        )

    # Decisive target assertion. One successful replacement must leave 4 rows:
    # 101 Austin + 104 Howard, while 102 and 103 remain untouched.
    run(["test", "--select", "regression_delete_insert_state"])

    print("\nPASS: Concurrent DELETE_INSERT final target state is valid", flush=True)
    print(
        f"PASS: successful executions={len(successes)} failed executions={len(failures)}",
        flush=True,
    )
    if failures:
        print(
            "PASS: stale overlapping execution was rejected before commit",
            flush=True,
        )
    else:
        print(
            "PASS: both executions serialized safely without corrupting target state",
            flush=True,
        )
    print("\nDELETE_INSERT CONCURRENCY REGRESSION PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nDELETE_INSERT CONCURRENCY REGRESSION FAILED: {exc}", flush=True)
        sys.exit(1)
