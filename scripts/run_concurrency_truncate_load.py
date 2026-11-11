import json
import shutil
import subprocess
import sys
import time

from run_regression_update import PROJECT, run


WAIT_SECONDS = 12
START_STAGGER_SECONDS = 0.75
PROCESS_TIMEOUT_SECONDS = 300


def _command(label, regression_case):
    target_path = PROJECT / f"target_concurrency_truncate_{label}"
    log_path = PROJECT / f"logs_concurrency_truncate_{label}"

    shutil.rmtree(target_path, ignore_errors=True)
    shutil.rmtree(log_path, ignore_errors=True)

    vars_payload = {
        "regression_case": regression_case,
        "regression_truncate_concurrency_wait_seconds": WAIT_SECONDS,
    }

    cmd = [
        "dbt", "run",
        "--select", "regression_customer_replace",
        "--vars", json.dumps(vars_payload),
        "--no-partial-parse",
        "--target-path", str(target_path),
        "--log-path", str(log_path),
    ]
    return cmd, target_path


def _start(label, regression_case):
    cmd, target_path = _command(label, regression_case)
    print(
        f"\nStarting concurrent TRUNCATE_LOAD run {label} ({regression_case}): "
        f"{' '.join(cmd)}",
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
        raise RuntimeError(f"Concurrent TRUNCATE_LOAD run {label} timed out")

    print(
        f"\n===== CONCURRENT TRUNCATE_LOAD RUN {label} | exit={proc.returncode} =====",
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
            "model_name": "regression_customer_replace",
            "target_identifier": "DIM_CUSTOMER_REPLACE_TEST",
            "check_name": "TRUNCATE_LOAD_TRANSACTION_CRITICAL_DQ",
            "expected_reason": "TARGET_CHANGED_SINCE_PRECHECK",
        }),
    ])


def main():
    print("TRUNCATE_LOAD SAME-COUNT CONCURRENCY REGRESSION", flush=True)
    print(
        "Goal: two independent full-table replacements start from the same 3-row "
        "baseline and each supply a different 3-row snapshot. Exactly one stale "
        "writer should be allowed to commit.",
        flush=True,
    )

    # Baseline also has 3 rows. Keeping the row count unchanged deliberately
    # tests whether count-only commit gates can detect stale full replacements.
    run(["run-operation", "regression_prepare_truncate_load_lock"])
    run(["run-operation", "regression_reset_replace"])
    print("PASS: TRUNCATE_LOAD concurrency baseline initialized", flush=True)

    proc_a, target_a = _start("A", "concurrency_same_count_a")
    time.sleep(START_STAGGER_SECONDS)
    proc_b, target_b = _start("B", "concurrency_same_count_b")

    rc_a, out_a = _finish("A", proc_a)
    rc_b, out_b = _finish("B", proc_b)

    outcomes = [
        ("A", rc_a, out_a, target_a),
        ("B", rc_b, out_b, target_b),
    ]

    for label, _, output, _ in outcomes:
        if "TRUNCATE_LOAD REGRESSION CONCURRENCY HOLD" not in output:
            raise RuntimeError(f"Run {label} did not enable the concurrency hold")

    successes = [x for x in outcomes if x[1] == 0]
    failures = [x for x in outcomes if x[1] != 0]

    # Always prove the final target is one complete snapshot, never a mix.
    run(["test", "--select", "regression_replace_concurrency_snapshot"])
    print("PASS: Final target is one complete replacement snapshot", flush=True)

    for label, _, output, _ in successes:
        if "TRUNCATE_LOAD DML COMMITTED" not in output:
            raise RuntimeError(
                f"Run {label} returned success without TRUNCATE_LOAD commit evidence"
            )

    for label, _, output, target_path in failures:
        if "TRUNCATE_LOAD_CRITICAL_DQ_FAILED" not in output:
            raise RuntimeError(
                f"Run {label} failed for an unexpected reason instead of the "
                "transaction-critical TRUNCATE_LOAD guard"
            )
        if "TRUNCATE_LOAD DML COMMITTED" in output:
            raise RuntimeError(
                f"Run {label} logged a commit despite transaction-critical failure"
            )
        _verify_failed_audit(_invocation_id(target_path))
        print(
            f"PASS: Run {label} failed safely and persisted TARGET_CHANGED_SINCE_PRECHECK audit",
            flush=True,
        )

    if len(successes) != 1 or len(failures) != 1:
        if len(successes) == 2:
            raise RuntimeError(
                "Both same-row-count full replacements committed. The TRUNCATE_LOAD "
                "stale-target fingerprint guard did not reject the second writer; "
                "full replacements are still last-writer-wins."
            )
        raise RuntimeError(
            f"Expected exactly one success and one safe rejection; "
            f"successes={len(successes)} failures={len(failures)}"
        )

    print("PASS: successful executions=1 failed executions=1", flush=True)
    print("PASS: stale full replacement was rejected before commit", flush=True)
    print("\nTRUNCATE_LOAD SAME-COUNT CONCURRENCY REGRESSION PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTRUNCATE_LOAD CONCURRENCY REGRESSION FAILED: {exc}", flush=True)
        sys.exit(1)
