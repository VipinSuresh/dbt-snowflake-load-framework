import json
import shutil
import subprocess
import sys
import time

from run_regression_update import PROJECT, run


WAIT_SECONDS = 12
START_STAGGER_SECONDS = 0.75
PROCESS_TIMEOUT_SECONDS = 240


def _concurrent_command(label, regression_case):
    target_path = PROJECT / f"target_concurrency_delete_insert_distinct_{label}"
    log_path = PROJECT / f"logs_concurrency_delete_insert_distinct_{label}"

    shutil.rmtree(target_path, ignore_errors=True)
    shutil.rmtree(log_path, ignore_errors=True)

    vars_payload = {
        "regression_case": regression_case,
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
    return cmd


def _start(label, regression_case):
    cmd = _concurrent_command(label, regression_case)
    print(
        f"\nStarting concurrent DELETE_INSERT run {label} ({regression_case}): {' '.join(cmd)}",
        flush=True,
    )
    return subprocess.Popen(
        cmd,
        cwd=PROJECT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _finish(label, proc):
    try:
        output, _ = proc.communicate(timeout=PROCESS_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        output, _ = proc.communicate()
        print(output, flush=True)
        raise RuntimeError(f"Concurrent DELETE_INSERT run {label} timed out")

    print(
        f"\n===== DISTINCT-KEY DELETE_INSERT RUN {label} | exit={proc.returncode} =====",
        flush=True,
    )
    print(output, flush=True)
    return proc.returncode, output


def _parse_post_commit_failures(output):
    markers = (
        "Post-load DQ failed after commit. Failed checks:",
        "Post-load DQ failed after commit:",
    )
    for marker in markers:
        if marker in output:
            tail = output.split(marker, 1)[1].splitlines()[0]
            return [x.strip() for x in tail.split(",") if x.strip()]
    return []


def main():
    print("DELETE_INSERT DISTINCT-KEY CONCURRENCY REGRESSION", flush=True)
    print(
        "Goal: two independent dbt/Snowflake sessions replace disjoint key sets in "
        "the same DELETE_INSERT target. Both valid changes should coexist.",
        flush=True,
    )

    # Deterministic 3-row baseline: 101/102/103.
    run(["run-operation", "regression_reset_delete_insert"])
    print("PASS: DELETE_INSERT distinct-key concurrency baseline initialized", flush=True)

    # Run A owns keys {101,104}; Run B owns keys {102,105}.
    # Each run has net row delta +1, intentionally pressuring whole-table
    # post-load row-count reconciliation under legitimate concurrency.
    proc_a = _start("A", "concurrency_distinct_a")
    time.sleep(START_STAGGER_SECONDS)
    proc_b = _start("B", "concurrency_distinct_b")

    rc_a, out_a = _finish("A", proc_a)
    rc_b, out_b = _finish("B", proc_b)

    for label, output in (("A", out_a), ("B", out_b)):
        if "DELETE_INSERT REGRESSION CONCURRENCY HOLD" not in output:
            raise RuntimeError(f"Run {label} did not enable the concurrency hold")

    # Validate data even if one dbt command reports a post-commit DQ failure.
    final_state_ok = True
    try:
        run(["test", "--select", "regression_delete_insert_distinct_keys"])
        print(
            "PASS: Both distinct-key DELETE_INSERT changes are present with valid final state",
            flush=True,
        )
    except Exception:
        final_state_ok = False

    if not final_state_ok:
        raise RuntimeError(
            "Distinct-key concurrent DELETE_INSERT executions lost or corrupted final target state"
        )

    failures = []
    for label, rc, output in (("A", rc_a, out_a), ("B", rc_b, out_b)):
        if rc == 0:
            if "DELETE_INSERT DML COMMITTED" not in output:
                failures.append((label, "SUCCESS_WITHOUT_COMMIT_EVIDENCE", []))
            continue

        if "Post-load DQ failed after commit" in output:
            failures.append((label, "POST_COMMIT_DQ", _parse_post_commit_failures(output)))
        elif "DELETE_INSERT_CRITICAL_DQ_FAILED" in output:
            failures.append((label, "TRANSACTION_CRITICAL_DQ", []))
        else:
            failures.append((label, "UNEXPECTED", []))

    if failures:
        details = "; ".join(
            f"Run {label}: {kind} ({','.join(checks) if checks else 'no check parsed'})"
            for label, kind, checks in failures
        )
        raise RuntimeError(
            "Final DELETE_INSERT data is valid, but legitimate distinct-key concurrency "
            f"caused framework execution failure: {details}"
        )

    print("PASS: Run A succeeded", flush=True)
    print("PASS: Run B succeeded", flush=True)
    print(
        "PASS: Legitimate distinct-key DELETE_INSERT concurrency produced no false DQ failure",
        flush=True,
    )
    print("\nDELETE_INSERT DISTINCT-KEY CONCURRENCY REGRESSION PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"\nDELETE_INSERT DISTINCT-KEY CONCURRENCY REGRESSION FAILED: {exc}",
            flush=True,
        )
        sys.exit(1)
