# Enterprise dbt + Snowflake Load Framework

A configuration-driven **dbt + Snowflake reference implementation** for loading existing, governed target tables with six reusable load strategies, schema contracts, execution auditing, transactional commit gates, rollback-safe failure handling, and regression-tested concurrency behavior.

The project is not primarily about writing six SQL loading patterns. It demonstrates the engineering problems that appear when those patterns are placed behind a common enterprise-style loading interface:

- defining safe transaction boundaries
- validating governed schemas before DML
- separating commit gates from post-load observability
- preserving failure evidence across rollback
- handling stale concurrent executions
- detecting corrupted target state
- scoping reconciliation to execution-owned changes
- serializing full replacements when the target may be empty

> **Scope:** This repository is an enterprise-style reference implementation, not a drop-in production package for every Snowflake environment.

This framework is most relevant to governed data platforms that need consistent loading, validation, audit, and failure-handling behavior across existing Snowflake targets. Its value comes from repeated operational and governance requirements across workloads, not from the size of the development team. It is not intended to replace native dbt materializations where those already satisfy the operational requirements.

---

## What This Project Demonstrates

A model selects a loading strategy through configuration while the custom `enterprise_load` materialization provides the common execution lifecycle.

```text
Model SQL
   |
   v
Framework configuration
   |
   v
Prepare execution source
   |
   v
Pre-load validation
   |
   v
Load executor
   |
   +---- multi-step strategies -------------------+
   |                                              |
   |   BEGIN TRANSACTION                          |
   |        |                                     |
   |        v                                     |
   |      DML                                     |
   |        |                                     |
   |        v                                     |
   |   Transaction-critical commit gates          |
   |        |                                     |
   |     PASS / FAIL                              |
   |      |       |                               |
   |      |       +-----------------> ROLLBACK    |
   |      v                                       |
   |    COMMIT                                    |
   +----------------------------------------------+
   |
   v
Post-load reconciliation
   |
   v
Audit finalization
```

The key design distinction is:

**Transaction-critical validation**  
Small invariants that determine whether a multi-step business transaction is allowed to commit.

**Post-load reconciliation**  
Broader validation and observability that runs after the transaction commits.

This keeps expensive checks out of the critical transaction where possible while still preserving strong correctness checks before commit.

---

## Supported Load Strategies

| Strategy | Behavior |
|---|---|
| `TYPE1` | Inserts new rows and updates existing rows |
| `TYPE2` | Maintains SCD Type 2 history |
| `INSERT` | Inserts new rows without updating existing rows |
| `UPDATE` | Updates existing rows without inserting new rows |
| `DELETE_INSERT` | Deletes matching target rows and inserts replacement rows |
| `TRUNCATE_LOAD` | Replaces the complete target dataset |

`TRUNCATE_LOAD` does **not** execute Snowflake DDL `TRUNCATE`. It uses transactional `DELETE + INSERT` so the replacement can participate in rollback behavior.

---

## Configuration Example

Model behavior is configured in YAML rather than implemented through separate custom materializations for each loading pattern.

```yaml
version: 2

models:
  - name: dim_customer
    config:
      materialized: enterprise_load
      schema: MARTS

      # Optional: use dbt's native alias when the governed physical
      # table identifier differs from the dbt model name.
      alias: DIM_CUSTOMER

      meta:
        enterprise_load:
          load_type: TYPE1

          natural_keys:
            - CUSTOMER_ID

          processing_window:
            enabled: true
            date_column: SOURCE_UPDATED_AT

          pre_load:
            column_match: true
            require_nonempty: true
            validate_keys: true

            volume_threshold:
              enabled: true
              max_drop_pct: 40
              max_growth_pct: 100

          post_load:
            reconcile_counts: true
            duplicate_check: true
            source_target_match: true
```

The framework operates on dbt's resolved `this` relation. Database, schema, and physical identifier therefore remain aligned with dbt's normal relation resolution. When a governed table name differs from the model name, dbt's native `alias` configuration can be used instead of introducing a second target-resolution mechanism.

---

## Design Principles

The implementation is built around the following principles:

- one reusable loading interface for multiple load patterns
- configuration-driven behavior
- existing governed targets rather than automatic target creation
- schema and datatype validation before DML
- standardized execution and audit metadata
- explicit transactions for multi-step loading
- rollback on critical failures
- lightweight commit-gate validation inside transactions
- broader reconciliation after commit
- concurrency protection for high-risk multi-step operations
- repeatable positive, negative, edge-case, and concurrency regressions

The materialization intentionally does **not** `CREATE OR REPLACE` governed final targets.

---

## Architecture

| Component | Responsibility |
|---|---|
| `enterprise_load.sql` | Custom materialization and strategy routing |
| `framework_config.sql` | Framework configuration and validation |
| `execution_context.sql` | Run-level execution context and execution numbering |
| `enterprise_control_relations.sql` | Control-object relation resolution |
| `load_executors.sql` | Execution lifecycle for the six strategies |
| `load_strategies.sql` | Strategy-specific SQL generation |
| `snowflake_dml.sql` | Snowflake DML execution utilities |
| `pre_load_checks.sql` | Pre-load contract and data validation |
| `post_load_checks.sql` | Post-load reconciliation |
| `audit_tracking.sql` | Model statistics and DQ audit persistence |
| `processing_window.sql` | Optional processing-window validation |
| `macros/regression/` | Regression setup and verification helpers |

---

## Pre-Load Validation

The framework validates source and target state before modifying governed data.

Shared checks include:

- source and target relation availability
- source/target column contract
- datatype compatibility
- natural-key configuration
- source non-empty requirement
- NULL natural keys
- duplicate source keys
- required business-column NULL checks
- existing target NULL keys
- existing target duplicate keys
- optional source-volume thresholds
- optional processing-window validation

Schema incompatibility is rejected before business DML begins.

### TYPE2 history validation

TYPE2 adds historical-state checks including:

- required SCD metadata
- exactly one current row per key
- valid history windows
- unique version numbers
- history overlap
- history continuity
- missing-current conditions

This means the framework does not assume an existing history table is valid simply because the incoming source is valid.

---

## Transactional Loading

The multi-step strategies use explicit Snowflake transactions:

- `TYPE2`
- `DELETE_INSERT`
- `TRUNCATE_LOAD`

Conceptually:

```text
BEGIN

DML step 1
DML step 2

transaction-critical validation

COMMIT

EXCEPTION
    ROLLBACK
    RAISE
```

This prevents partial business-state changes when a later DML step or transaction-critical validation fails.

Temporary execution objects are prepared outside the business transaction so DDL does not interfere with the intended transaction boundary.

---

## TYPE1

`TYPE1` inserts new natural keys and updates existing rows through a single load operation.

The framework validates:

- relation and schema contracts
- source and target key quality
- required business columns
- optional volume thresholds

Post-load checks can reconcile row-count behavior, duplicate-key state, and source/target matching.

---

## TYPE2

TYPE2 processing prepares an immutable execution delta containing rows that are:

- new
- changed

Changed current records are expired and new versions are inserted within the same business transaction.

The framework maintains:

```text
VALID_FROM
VALID_TO
IS_CURRENT
VERSION_NO
```

Transaction-critical checks protect the history from invalid execution-owned state before commit. Broader history reconciliation runs after commit.

This separation matters under concurrency: a stale execution should fail before committing conflicting history, while unrelated concurrent changes should not create false failures.

---

## INSERT

`INSERT` adds new natural keys without updating existing target rows.

The strategy is intended for append-like governed targets where existing keys should remain unchanged.

Regression coverage verifies positive inserts, execution audit behavior, and protection against invalid source state.

---

## UPDATE

`UPDATE` modifies existing natural keys without inserting missing rows.

Post-load validation confirms that the operation does not introduce unintended inserts or deletes and can reconcile execution-owned updates against the source.

---

## DELETE_INSERT

`DELETE_INSERT` identifies target rows matching the incoming natural keys.

Within one transaction:

```text
DELETE matching target rows
INSERT prepared source rows
validate transaction counts
COMMIT
```

If a failure occurs after the delete but before the transaction commits, the delete is rolled back.

Concurrency validation is scoped to the affected execution state so unrelated distinct-key writers are not rejected by whole-table changes.

---

## TRUNCATE_LOAD

`TRUNCATE_LOAD` performs full replacement using:

```text
DELETE FROM target
INSERT replacement snapshot
```

rather than Snowflake DDL `TRUNCATE`.

A full replacement can begin against an empty target, so locking only existing target rows is not sufficient for serialization.

The framework therefore uses a framework-owned control row:

```text
ETL_CONTROL.ETL_FRAMEWORK_LOCKS
TRUNCATE_LOAD_GLOBAL
```

Competing full-replacement executions serialize on this control state before the governed target is modified.

Full-replacement loads are serialized through a framework-owned global lock. This is a deliberate correctness-first choice. More granular per-target serialization would require a different coordination design and additional implementation choices, so it was intentionally left outside this reference implementation.

---

## Failure Handling

The framework distinguishes controlled validation failures from unexpected execution failures.

### Controlled transaction-critical failure

When a known commit-gate condition fails:

```text
business transaction
    -> ROLLBACK

separate audit transaction
    -> persist failure evidence

re-raise controlled failure
```

This allows failure audit evidence to survive rollback of the business DML.

### Unexpected SQL/runtime failure

Unexpected Snowflake/database errors are rolled back by the transaction exception handler and re-raised.

The dbt end hook conservatively records:

```text
LOAD_OUTCOME_UNKNOWN
```

rather than claiming certainty about the outcome of an arbitrary database failure.

That behavior is deliberate: the audit layer should not infer a database outcome it did not independently verify.

---

## Audit Model

The framework maintains run-level, model-level, and DQ-level execution evidence.

| Object | Purpose |
|---|---|
| `ETL_RUN_CONTROL` | dbt invocation/run metadata |
| `ETL_MODEL_STATS` | model-level load statistics |
| `ETL_DQ_RESULTS` | DQ and validation evidence |
| `ETL_NR_SEQ` | execution-number sequence |
| `ETL_FRAMEWORK_LOCKS` | framework serialization control |

Control-object setup is provided in:

```text
setup/create_etl_control_objects.sql
```

The setup script creates missing framework control objects without creating arbitrary governed business targets.

---

## Standardized Target Audit Columns

The public demo uses eight standardized target audit fields:

```text
CREATED_BY_PROCESS
CREATED_AT
ETL_CREATED_RUN_NUMBER
ETL_CREATED_AT
UPDATED_BY_PROCESS
UPDATED_AT
ETL_UPDATED_RUN_NUMBER
ETL_UPDATED_AT
```

These fields associate target rows with framework executions while keeping the public example independent of organization-specific naming conventions.

---

## Execution Context

The framework supports run identification and execution metadata for local dbt Core execution and dbt Cloud-style execution context.

Run lifecycle hooks initialize and finalize framework tracking:

```yaml
on-run-start:
  - "{{ start_etl_run() }}"

on-run-end:
  - "{{ finish_etl_run() }}"
```

Control schemas are resolved through project variables:

```yaml
vars:
  enterprise_control_schema: ETL_CONTROL
  enterprise_regression_schema: REGRESSION_TEST
```

---

## Processing Window

Models may declare an optional processing window configuration.

```yaml
processing_window:
  enabled: true
  date_column: SOURCE_UPDATED_AT
```

The framework validates the declared runtime window and records the validation result. The window declaration itself does not automatically rewrite model SQL; source filtering remains the responsibility of the model.

`TRUNCATE_LOAD` prohibits an enabled processing window because it represents a complete replacement rather than a partial-window load.

---

## Concurrency

Concurrency testing focuses on the multi-step strategies where stale execution state creates the highest correctness risk.

### TYPE2

Testing covers:

- competing runs affecting the same natural key
- competing runs affecting distinct keys
- stale delta/version scenarios

Same-key stale writers are rejected before commit.

Distinct-key runs are not incorrectly failed by reconciliation belonging to unrelated concurrent state.

### DELETE_INSERT

Testing covers:

- overlapping-key concurrent execution
- distinct-key concurrent execution

Validation is scoped to execution-owned or affected-key state rather than unrelated concurrent changes.

### TRUNCATE_LOAD

Testing covers:

- concurrent same-count replacement
- empty-target replacement
- stale-target detection

The framework serialization lock prevents two full-replacement writers from independently treating the same baseline as valid.

Concurrency behavior for single-statement `TYPE1`, `INSERT`, and `UPDATE` has not been separately regression-tested in this project.

---

## Regression Coverage

The repository includes regressions for all six supported strategies plus targeted reliability and edge-case suites.

Coverage includes:

- normal TYPE1 behavior
- normal TYPE2 behavior
- normal INSERT behavior
- normal UPDATE behavior
- normal DELETE_INSERT behavior
- normal TRUNCATE_LOAD behavior
- transaction rollback
- transaction-critical DQ failure
- failure audit persistence
- TYPE2 same-key concurrency
- TYPE2 distinct-key concurrency
- DELETE_INSERT overlapping-key concurrency
- DELETE_INSERT distinct-key concurrency
- TRUNCATE_LOAD concurrency
- empty-target full-replacement serialization
- composite natural keys
- NULL composite-key components
- duplicate composite keys
- pre-corrupted target state
- TYPE2 duplicate-current corruption
- TYPE2 duplicate-version corruption
- TYPE2 overlapping history
- schema drift
- missing source columns
- unexpected source columns
- incompatible datatypes
- TYPE2 reserved metadata columns
- unexpected SQL/runtime failures

The aggregate standard regression runner is:

```bash
python scripts/run_all_regressions.py
```

Additional targeted runners are available under `scripts/`.

Examples include:

```text
run_concurrency_type2.py
run_concurrency_type2_distinct_keys.py
run_concurrency_delete_insert.py
run_concurrency_delete_insert_distinct_keys.py
run_concurrency_truncate_load.py
run_concurrency_truncate_load_empty.py
run_edge_composite_keys.py
run_edge_schema_drift.py
run_edge_target_corruption.py
run_edge_runtime_failure.py
```

---

## Project Structure

```text
.
├── macros/
│   ├── enterprise_load.sql
│   ├── framework_config.sql
│   ├── execution_context.sql
│   ├── enterprise_control_relations.sql
│   ├── load_executors.sql
│   ├── load_strategies.sql
│   ├── snowflake_dml.sql
│   ├── pre_load_checks.sql
│   ├── post_load_checks.sql
│   ├── audit_tracking.sql
│   ├── processing_window.sql
│   └── regression/
│
├── models/
│   ├── demo/
│   └── regression/
│
├── tests/
├── scripts/
├── setup/
├── selectors.yml
└── dbt_project.yml
```

---

## Setup

### 1. Configure a Snowflake dbt profile

Configure the project to connect to the intended development Snowflake environment.

### 2. Create the framework control objects

Execute:

```text
setup/create_etl_control_objects.sql
```

Select the intended database first and ensure the configured control schema matches `enterprise_control_schema`.

### 3. Provision source and governed target tables

The framework expects governed targets to exist before a model runs.

It intentionally does not create or replace arbitrary business target tables.

### 4. Configure the load strategy

Set:

- `materialized: enterprise_load`
- the desired `load_type`
- `natural_keys`
- applicable pre-load controls
- applicable post-load controls
- optional processing-window metadata

### 5. Validate dbt connectivity and execute

```bash
dbt debug
dbt run
```

For the bundled regression suite:

```bash
python scripts/run_all_regressions.py
```

---

## Environment Used for Validation

The framework has been exercised locally using:

```text
Python 3.11
dbt Core 1.12.6
dbt-snowflake 1.12.4
Snowflake
```

Other dbt Core, adapter, Python, or Snowflake environment combinations have not been systematically compatibility-tested.

---

## Important Limitations

This repository demonstrates framework architecture and correctness patterns. It is not presented as a universally production-ready package.

Current boundaries include:

- no systematic large-scale performance benchmark
- no compatibility matrix across dbt/dbt-snowflake versions
- no dedicated concurrency regression for `TYPE1`, `INSERT`, or `UPDATE`
- no dedicated regression specifically for empty-source behavior
- no exhaustive late-arriving TYPE2 scenario suite
- `TRUNCATE_LOAD` currently uses global serialization; more granular per-target serialization would require a different coordination design and additional implementation choices
- deployment, permissions, monitoring, alerting, and operational controls must be adapted to the target organization

These are explicit boundaries of the current reference implementation rather than claims of behavior that has not been tested.

---

## When Not to Use This

This framework is not the best fit for every dbt project.

It may be unnecessary when:

- native dbt incremental models or snapshots already satisfy the loading and governance requirements
- the project is greenfield and dbt fully owns target-table lifecycle and DDL
- the workload requires very low-latency, high-frequency processing where per-run validation and audit overhead would be inappropriate
- large-scale performance is a primary requirement and the framework has not yet been benchmarked at that scale
- the team prefers minimal custom materialization logic and lower framework-maintenance overhead

---

## Why the Reliability Layer Matters

A load strategy is easy to describe when only the happy path is considered.

The difficult engineering questions are different:

- What state is allowed to commit?
- Which checks belong inside the transaction?
- Which checks should run after commit?
- What happens when step two fails after step one succeeds?
- How is failure evidence retained after rollback?
- What happens when two runs start from the same baseline?
- How should validation distinguish execution-owned state from unrelated concurrent changes?
- How should a full replacement serialize when the target is empty?
- What should the audit layer claim after an unexpected database error?

Those concerns are what turn a collection of SQL statements into a reusable data-engineering framework.
