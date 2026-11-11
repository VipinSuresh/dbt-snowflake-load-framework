import json
import shutil
import subprocess
import sys
import time

from run_regression_update import PROJECT, run


POST_RECHECK_WAIT_SECONDS = 12
START_STAGGER_SECONDS = 0.25
PROCESS_TIMEOUT_SECONDS = 300


def _command(label, regression_case):
    target_path = PROJECT / f"target_concurrency_truncate_empty_{label}"
    log_path = PROJECT / f"logs_concurrency_truncate_empty_{label}"

    shutil.rmtree(target_path, ignore_errors=True)
    shutil.rmtree(log_path, ignore_errors=True)

    vars_payload = {
        "regression_case": regression_case,
        "regression_truncate_post_recheck_wait_seconds": POST_RECHECK_WAIT_SECONDS,
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
        f"\nStarting concurrent empty-baseline TRUNCATE_LOAD run {label} "
        f"({regression_case}): {' '.join(cmd)}",
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
        raise RuntimeError(f"Concurrent empty TRUNCATE_LOAD run {label} timed out")

    print(
        f"\n===== EMPTY-BASELINE TRUNCATE_LOAD RUN {label} | exit={proc.returncode} =====",
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


def _verify_failed_audit(invocation_id, expected_reason):
    run([
        "run-operation",
        "regression_verify_transaction_dq_failure",
        "--args",
        json.dumps({
            "invocation_id": invocation_id,
            "model_name": "regression_customer_replace",
            "target_identifier": "DIM_CUSTOMER_REPLACE_TEST",
            "check_name": "TRUNCATE_LOAD_TRANSACTION_CRITICAL_DQ",
            "expected_reason": expected_reason,
        }),
    ])


def _run_snapshot_test():
    result = subprocess.run(
        [
            "dbt", "test", "--select", "regression_replace_concurrency_snapshot",
            "--no-partial-parse",
        ],
        cwd=PROJECT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print("\nRunning: test --select regression_replace_concurrency_snapshot", flush=True)
    print(result.stdout, flush=True)
    return result.returncode == 0


def main():
    print("TRUNCATE_LOAD EMPTY-BASELINE CONCURRENCY REGRESSION", flush=True)
    print(
        "Goal: force two independent full replacements to both observe an empty "
        "target before either reaches DELETE. The framework must still allow only "
        "one replacement to commit.",
        flush=True,
    )

    run(["run-operation", "regression_prepare_truncate_load_lock"])
    run(["run-operation", "regression_reset_replace_empty"])
    run(["test", "--select", "regression_replace_empty_baseline"])
    print("PASS: TRUNCATE_LOAD empty concurrency baseline initialized", flush=True)

    proc_a, target_a = _start("A", "concurrency_same_count_a")
    time.sleep(START_STAGGER_SECONDS)
    proc_b, target_b = _start("B", "concurrency_same_count_b")

    rc_a, out_a = _finish("A", proc_a)
    rc_b, out_b = _finish("B", proc_b)

    outcomes = [
        ("A", rc_a, out_a, target_a),
        ("B", rc_b, out_b, target_b),
    ]

    successes = [x for x in outcomes if x[1] == 0]
    failures = [x for x in outcomes if x[1] != 0]

    final_snapshot_valid = _run_snapshot_test()
    if final_snapshot_valid:
        print("PASS: Final target is one complete replacement snapshot", flush=True)
    else:
        print(
            "FAIL: Final target is not one complete replacement snapshot; concurrent "
            "empty-target replacements produced combined/invalid state",
            flush=True,
        )

    for label, _, output, _ in successes:
        if "TRUNCATE_LOAD DML COMMITTED" not in output:
            raise RuntimeError(
                f"Run {label} returned success without TRUNCATE_LOAD commit evidence"
            )

    controlled_failures = []
    post_commit_failures = []
    unexpected_failures = []

    for label, _, output, target_path in failures:
        if "TRUNCATE_LOAD_CRITICAL_DQ_FAILED" in output:
            # The framework control row is the serialization point even when
            # the governed target is empty. The loser should acquire the lock
            # only after the winner commits, observe a changed lock version, and
            # fail before DELETE/INSERT.
            _verify_failed_audit(
                _invocation_id(target_path),
                "TARGET_CHANGED_SINCE_PRECHECK",
            )
            controlled_failures.append((label, "TARGET_CHANGED_SINCE_PRECHECK"))
            print(
                f"PASS: Run {label} failed safely and persisted "
                "TARGET_CHANGED_SINCE_PRECHECK audit",
                flush=True,
            )
        elif "Post-load DQ failed after commit" in output or "Post-load DQ failed" in output:
            post_commit_failures.append(label)
        else:
            unexpected_failures.append((label, "unexpected dbt failure"))

    if unexpected_failures:
        raise RuntimeError(f"Unexpected failures: {unexpected_failures}")

    if len(successes) == 1 and len(controlled_failures) == 1 and final_snapshot_valid:
        print("PASS: successful executions=1 failed executions=1", flush=True)
        print("PASS: empty-target stale full replacement was rejected safely", flush=True)
        print("\nTRUNCATE_LOAD EMPTY-BASELINE CONCURRENCY REGRESSION PASSED", flush=True)
        return

    if len(successes) == 2:
        raise RuntimeError(
            "Both empty-target full replacements committed. The framework control-row "
            "serialization guard did not reject the stale first-load writer."
        )

    if post_commit_failures:
        raise RuntimeError(
            "A concurrent empty-target replacement committed and only failed in "
            f"post-load DQ ({','.join(post_commit_failures)}). Rejection is too late."
        )

    if not final_snapshot_valid:
        raise RuntimeError(
            "Concurrent empty-target replacements left invalid combined state. The "
            "framework control-row serialization did not protect the zero-row target."
        )

    raise RuntimeError(
        f"Expected one success and one controlled transaction rejection; "
        f"successes={len(successes)} controlled_failures={len(controlled_failures)}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTRUNCATE_LOAD EMPTY CONCURRENCY REGRESSION FAILED: {exc}", flush=True)
        sys.exit(1)
