# Enterprise dbt + Snowflake Load Framework

A configuration-driven dbt framework for loading existing, governed Snowflake tables with standardized execution behavior, data-quality controls, audit tracking, transactional safety, and regression testing.

The project demonstrates how multiple enterprise loading patterns can be implemented behind a common dbt materialization instead of maintaining separate one-off loading implementations for every model.

## Supported Load Strategies

| Strategy | Behavior |
|---|---|
| `TYPE1` | Inserts new rows and updates existing rows |
| `TYPE2` | Maintains SCD Type 2 history |
| `INSERT` | Inserts new rows without updating existing rows |
| `UPDATE` | Updates existing rows without inserting new rows |
| `DELETE_INSERT` | Deletes matching target rows and inserts replacement rows |
| `TRUNCATE_LOAD` | Replaces the complete target dataset |

`TRUNCATE_LOAD` does not issue a Snowflake `TRUNCATE` statement.

It uses transactional `DELETE + INSERT` so the replacement can participate in rollback behavior.

---

## Design Goals

The framework is designed around several principles:

- one reusable loading interface for multiple load patterns
- configuration-driven behavior
- existing governed target tables rather than automatic target creation
- schema and datatype validation before DML
- standardized execution and audit metadata
- transaction-safe multi-step loading
- rollback on critical failures
- lightweight commit-gate validation inside transactions
- broader reconciliation after commit
- concurrency protection for high-risk multi-step operations
- repeatable regression testing

---

## Architecture

The main framework components are:

| Component | Responsibility |
|---|---|
| `enterprise_load.sql` | Custom materialization and strategy routing |
| `framework_config.sql` | Framework configuration and validation |
| `execution_context.sql` | Run-level execution context |
| `enterprise_control_relations.sql` | Control-object relation resolution |
| `load_executors.sql` | Execution logic for the six strategies |
| `load_strategies.sql` | Strategy-specific SQL generation |
| `snowflake_dml.sql` | Snowflake DML utilities |
| `pre_load_checks.sql` | Pre-load contract and data validation |
| `post_load_checks.sql` | Post-load reconciliation |
| `audit_tracking.sql` | Model statistics and DQ audit persistence |
| `processing_window.sql` | Optional processing-window validation |

The framework operates against **existing physical Snowflake target tables**.

It intentionally does not `CREATE OR REPLACE` governed final targets.

---

## Execution Flow

A typical framework execution follows this pattern:

```text
Model SQL
   |
   v
Prepare immutable source
   |
   v
Pre-load validation
   |
   v
Execute load strategy
   |
   +---- transactional strategies ----+
   |                                  |
   |    BEGIN TRANSACTION             |
   |          |                       |
   |          v                       |
   |        DML                       |
   |          |                       |
   |          v                       |
   |    Critical commit gates         |
   |          |                       |
   |      PASS / FAIL                 |
   |       |       |                  |
   |       |       +--> ROLLBACK      |
   |       v                          |
   |     COMMIT                       |
   +----------------------------------+
   |
   v
Post-load reconciliation
   |
   v
Audit finalization
```

The important distinction is between:

**Transaction-critical validation**

Small invariants that determine whether a transaction is allowed to commit.

and:

**Post-load reconciliation**

Broader validation and observability that runs after the business transaction commits.

This keeps expensive validation out of the critical transaction where possible.

---

## Pre-Load Validation

The framework validates the source and target before modifying governed data.

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

TYPE2 additionally validates historical-state integrity, including:

- required SCD metadata
- exactly one current row per key
- valid history windows
- unique version numbers
- history overlap
- history continuity
- missing-current conditions

Schema incompatibility is rejected before business DML begins.

---

## Transactional Loading

The multi-step strategies use explicit Snowflake transactions:

- `TYPE2`
- `DELETE_INSERT`
- `TRUNCATE_LOAD`

Example:

```text
BEGIN

DELETE / UPDATE
INSERT
critical validation

COMMIT

EXCEPTION
    ROLLBACK
    RAISE
```

This prevents partial business-state changes when the second step or a transaction-critical validation fails.

Temporary objects used to prepare execution state are created outside the business transaction so DDL does not interfere with the intended transaction boundary.

---

## TYPE2

TYPE2 processing prepares an immutable execution delta containing the rows that are:

- new
- changed

Changed current records are expired and new versions are inserted in the same transaction.

The framework maintains:

- `VALID_FROM`
- `VALID_TO`
- `IS_CURRENT`
- `VERSION_NO`

Transaction-critical checks protect the history from conditions such as duplicate versions or invalid execution-owned state before commit.

Broader history reconciliation runs after commit.

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

If the transaction fails after the delete but before the insert completes, the delete is rolled back.

---

## TRUNCATE_LOAD

The strategy performs full replacement using:

```text
DELETE FROM target
INSERT replacement snapshot
```

instead of Snowflake DDL `TRUNCATE`.

Because a full replacement can begin against an empty target, relying only on locking existing target rows is insufficient.

The framework therefore uses a framework-owned serialization control row:

```text
ETL_CONTROL.ETL_FRAMEWORK_LOCKS
TRUNCATE_LOAD_GLOBAL
```

The control row serializes competing full replacements before the governed target is modified.

The current implementation intentionally uses a coarse global lock in favor of correctness and simplicity.

---

## Failure Handling

The framework distinguishes between controlled validation failures and unexpected execution failures.

### Controlled transaction-critical failure

When a known commit-gate condition fails:

```text
business transaction
    -> ROLLBACK

separate audit transaction
    -> persist failure evidence

re-raise original controlled failure
```

This allows failure audit evidence to survive the rollback of business DML.

### Unexpected SQL/runtime failure

Unexpected Snowflake/database errors are rolled back by the transaction exception handler and re-raised.

The dbt end hook conservatively records the model status as:

```text
LOAD_OUTCOME_UNKNOWN
```

rather than assuming the exact outcome of an arbitrary database failure.

This is intentional: the audit layer should not claim certainty that it does not possess.

---

## Audit Model

The framework maintains run-level, model-level, and DQ-level execution evidence.

Primary control objects:

| Object | Purpose |
|---|---|
| `ETL_RUN_CONTROL` | dbt invocation/run metadata |
| `ETL_MODEL_STATS` | model-level load statistics |
| `ETL_DQ_RESULTS` | DQ and validation evidence |
| `ETL_NR_SEQ` | execution-number sequence |
| `ETL_FRAMEWORK_LOCKS` | framework serialization control |

Control-object setup is provided under:

```text
setup/create_etl_control_objects.sql
```

---

## Standardized Target Audit Columns

The public demo uses standardized target audit metadata including:

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

These columns allow loaded records to be associated with framework executions.

---

## Concurrency

Concurrency testing focused on the multi-step strategies where stale execution state can cause the most serious correctness problems.

### TYPE2

Testing covered:

- competing runs affecting the same natural key
- competing runs affecting distinct keys
- stale delta/version scenarios

Same-key stale writers are rejected before commit.

Distinct-key runs are not incorrectly failed by whole-table reconciliation belonging to another execution.

### DELETE_INSERT

Testing covered:

- overlapping-key concurrent execution
- distinct-key concurrent execution

Validation is scoped to execution-owned or affected-key state rather than unrelated concurrent changes.

### TRUNCATE_LOAD

Testing covered:

- concurrent same-count replacement
- empty-target replacement
- stale-target detection

The framework serialization lock prevents two full-replacement writers from independently treating the same baseline as valid.

Concurrency behavior for single-statement `TYPE1`, `INSERT`, and `UPDATE` has not been separately regression-tested in this project.

---

## Regression Coverage

The project includes regression coverage for all six supported strategies.

The test suite includes:

- normal TYPE1 behavior
- normal TYPE2 behavior
- normal INSERT behavior
- normal UPDATE behavior
- normal DELETE_INSERT behavior
- normal TRUNCATE_LOAD behavior
- transaction rollback
- transaction-critical DQ failure
- failure audit persistence
- TYPE2 concurrency
- DELETE_INSERT concurrency
- TRUNCATE_LOAD concurrency
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

## Configuration

The project resolves framework control objects through dbt variables:

```yaml
vars:
  enterprise_control_schema: ETL_CONTROL
  enterprise_regression_schema: REGRESSION_TEST
```

Run lifecycle hooks initialize and finalize execution tracking:

```yaml
on-run-start:
  - "{{ start_etl_run() }}"

on-run-end:
  - "{{ finish_etl_run() }}"
```

Individual model behavior is driven through framework configuration rather than separate custom materializations for each loading strategy.

---

## Setup

1. Configure a Snowflake dbt profile.
2. Create the framework control objects using:

```text
setup/create_etl_control_objects.sql
```

3. Provision the required source and governed target tables.
4. Configure the appropriate load strategy and natural keys.
5. Run dbt normally.

Example:

```bash
dbt debug
dbt run
```

The setup SQL intentionally does not create arbitrary governed target tables.

---

## Important Limitations

This repository demonstrates framework architecture and correctness patterns. It is not presented as a drop-in production package for every Snowflake environment.

Current limitations include:

- no systematic large-scale performance benchmark
- no compatibility matrix across dbt/dbt-snowflake versions
- no dedicated concurrency regression for TYPE1, INSERT, or UPDATE
- no dedicated regression specifically for empty-source behavior
- no exhaustive late-arriving TYPE2 scenario suite
- TRUNCATE_LOAD currently uses coarse global serialization
- deployment, permissions, monitoring, alerting, and operational controls must be adapted to the target organization

These are intentional boundaries of the current project rather than claims of unsupported behavior.

---

## Why This Project Exists

The main goal is not simply to demonstrate six SQL loading patterns.

The project explores the engineering problems that appear when those patterns become a reusable enterprise framework:

- defining transaction boundaries
- separating commit gates from post-load observability
- preserving failure audit evidence across rollback
- handling stale concurrent executions
- validating governed schemas before DML
- detecting corrupted historical state
- designing execution-scoped reconciliation
- handling empty-target serialization
- responding safely to unexpected database errors

Those concerns are what turn a collection of SQL statements into a reusable data-engineering framework.
