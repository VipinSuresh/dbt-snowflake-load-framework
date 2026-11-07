Enterprise Loading Framework — v1 TYPE1 pre-load evidence / failure-state hardening

Files to COPY into your local Mac project (my_project/macros/):
  audit_tracking.sql       — REPLACE current file
  pre_load_checks.sql      — REPLACE current file
  enterprise_load.sql      — REPLACE current file
  execution_context.sql   — REPLACE current file (end hook now reconciles failed dbt models)

Keep your existing:
  framework_config.sql
  load_strategies.sql
  snowflake_dml.sql (main statement-name change already applied)
  post_load_checks.sql
  generate_schema_name.sql
  dbt_project.yml (same on-run-start and on-run-end hooks)
  models/marts/dim_customer.sql and schema.yml

Changes:
  1. ETL_MODEL_STATS row created ahead of temporary-source preparation/pre-load DQ.
  2. Each pre-load check inserts PASS/FAIL record into ETL_DQ_RESULTS.
  3. Failed pre-load checks update model LOAD_STATUS=PRE_LOAD_FAILED and DQ_STATUS=FAIL
     before raising a dbt error; governed target table is not loaded.
  4. The on-run-end hook repairs unfinished model statuses when dbt reports an error.
  5. Previously working TYPE1 SQL generation and post-load DQ are unchanged.

Run:
  dbt parse --no-partial-parse
  dbt build --select +dim_customer --no-partial-parse

Verify latest run:
  SELECT r.ETL_NR, r.RUN_STATUS, s.LOAD_STATUS, s.DQ_STATUS,
         s.SOURCE_ROW_COUNT, s.ROWS_INSERTED, s.ROWS_UPDATED
  FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL r
  JOIN DBT_DEMO.ETL_CONTROL.ETL_MODEL_STATS s USING (ETL_NR)
  ORDER BY r.RUN_STARTED_AT DESC LIMIT 5;

  SELECT CHECK_NAME, CHECK_PHASE, CHECK_STATUS, OBSERVED_VALUE,
         REFERENCE_VALUE, DETAILS
  FROM DBT_DEMO.ETL_CONTROL.ETL_DQ_RESULTS
  WHERE ETL_NR = (SELECT ETL_NR FROM DBT_DEMO.ETL_CONTROL.ETL_RUN_CONTROL
                  ORDER BY RUN_STARTED_AT DESC LIMIT 1)
  ORDER BY CHECK_PHASE, CHECK_NAME;

Expected clean rerun after Raj insert:
  source rows=5; new inserts=0, updates=0; model SUCCESS/DQ PASS.
  pre-load checks plus four existing post-load checks in DQ_RESULTS.

Negative test (AFTER positive run):
  Temporarily add a duplicate CUSTOMER_ID to the final dim_customer.sql SELECT
  by appending UNION ALL SELECT ... FROM {{ ref('int_customers') }}
  WHERE CUSTOMER_ID = 101. Use an explicit matching projection and keep the rest
  of the model unchanged. Run dbt build --select +dim_customer --no-partial-parse.
  Expected: dbt fails, ETL_MODEL_STATS PRE_LOAD_FAILED / DQ FAIL,
  ETL_DQ_RESULTS SOURCE_DUPLICATE_KEYS FAIL, MARTS.DIM_CUSTOMER unchanged.
  Restore original model SQL and run again.

Limitations:
  - Preliminary demo integration: not independently tested against your Snowflake account.
  - Jinja syntax parsed locally, but dbt adapter integration not executed here.
  - This phase does not implement historical thresholds, full type precision/length
    validation, transactional rollback for replacement strategies, or complete
    handling of process termination before on-run-end.
  - Snowflake transactional semantics determine whether records survive exceptions;
    verify failure-state persistence on the negative test.
  - This setup logs actual pre-load checks, but does not yet make all YAML DQ
    toggles functional (mandatory guards remain on).
  - The copied files replace rather than modify your current local Mac files.
