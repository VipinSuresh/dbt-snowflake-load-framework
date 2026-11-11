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


def _concurrent_command(label, regression_case):
    target_path = PROJECT / f"target_concurrency_distinct_{label}"
    log_path = PROJECT / f"logs_concurrency_distinct_{label}"

    shutil.rmtree(target_path, ignore_errors=True)
    shutil.rmtree(log_path, ignore_errors=True)

    vars_payload = {
        "regression_case": regression_case,
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
    return cmd


def _start(label, regression_case):
    cmd = _concurrent_command(label, regression_case)
    print(
        f"\nStarting concurrent TYPE2 run {label} ({regression_case}): {' '.join(cmd)}",
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
        raise RuntimeError(f"Concurrent TYPE2 run {label} timed out")

    print(f"\n===== DISTINCT-KEY TYPE2 RUN {label} | exit={proc.returncode} =====", flush=True)
    print(output, flush=True)
    return proc.returncode, output


def main():
    print("TYPE2 DISTINCT-KEY CONCURRENCY REGRESSION", flush=True)
    print(
        "Goal: two independent dbt/Snowflake sessions change different natural keys "
        "in the same TYPE2 target. Both changes are valid and should coexist.",
        flush=True,
    )

    run(["run-operation", "regression_reset_type2"])
    baseline = run([
        "run",
        "--select",
        "regression_customer_type2",
        "--vars",
        '{"regression_case":"initial"}',
    ])
    if "TYPE2 DML COMMITTED | inserted versions=3 | expired versions=0" not in baseline:
        raise RuntimeError("TYPE2 distinct-key baseline did not initialize correctly")
    run(["test", "--select", "regression_type2_initial"])
    print("PASS: TYPE2 distinct-key concurrency baseline initialized", flush=True)

    proc_a = _start("A", "changed_101")
    time.sleep(START_STAGGER_SECONDS)
    proc_b = _start("B", "changed_102")

    rc_a, out_a = _finish("A", proc_a)
    rc_b, out_b = _finish("B", proc_b)

    for label, output in (("A", out_a), ("B", out_b)):
        if "TYPE2 REGRESSION CONCURRENCY HOLD" not in output:
            raise RuntimeError(f"Run {label} did not enable the concurrency hold")

    # Check data integrity even if a dbt process reported a post-commit DQ error.
    # This distinguishes actual SCD2 corruption from concurrency-unsafe observability.
    final_state_ok = True
    try:
        run(["test", "--select", "regression_type2_distinct_keys"])
        print("PASS: Both distinct-key TYPE2 changes are present with valid history", flush=True)
    except Exception:
        final_state_ok = False

    failures = []
    for label, rc, output in (("A", rc_a, out_a), ("B", rc_b, out_b)):
        if rc != 0:
            if "Post-load DQ failed after commit" in output:
                failed_checks = []
                marker = "Post-load DQ failed after commit:"
                for line in output.splitlines():
                    if marker in line:
                        payload = line.split(marker, 1)[1].strip()
                        failed_checks = [
                            check.strip() for check in payload.split(',') if check.strip()
                        ]
                        break
                failures.append((label, "POST_COMMIT_DQ", failed_checks))
            elif "TYPE2_CRITICAL_DQ_FAILED" in output:
                failures.append((label, "TRANSACTION_CRITICAL_DQ", []))
            else:
                failures.append((label, "UNEXPECTED", []))

    if not final_state_ok:
        raise RuntimeError("Distinct-key concurrent executions corrupted or lost TYPE2 history")

    if failures:
        details = "; ".join(
            f"Run {label}: {kind} ({','.join(checks) if checks else 'no check parsed'})"
            for label, kind, checks in failures
        )
        raise RuntimeError(
            "Final TYPE2 data is valid, but legitimate different-key concurrency caused "
            f"framework execution failure: {details}"
        )

    print("PASS: Run A succeeded", flush=True)
    print("PASS: Run B succeeded", flush=True)
    print("PASS: Legitimate different-key concurrency produced no false DQ failure", flush=True)
    print("\nTYPE2 DISTINCT-KEY CONCURRENCY REGRESSION PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nTYPE2 DISTINCT-KEY CONCURRENCY REGRESSION FAILED: {exc}", flush=True)
        sys.exit(1)
