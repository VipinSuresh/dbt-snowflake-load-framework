# Enterprise dbt macro consolidation (relocation-only)

## Result
- Consolidates **29 active .sql macro files** into **11 .sql macro files**.
- Preserves **52 macro definitions**, their complete bodies and signatures exactly.
- Preserves the Snowflake `enterprise_load` materialization byte-for-byte.
- No strategy, audit, DQ, transaction, naming, or configuration logic modified.
- Existing models and dbt_project.yml intentionally left untouched.
- Demo macros preserved in `dev_probes.sql`; existing demo macros inside other macro files kept unchanged.
- `update_load.sql.bad_copy` and `.gitkeep` not included.

## Directory organization
- `enterprise_load.sql` — dispatcher/materialization (unchanged)
- `framework_config.sql` — configuration (unchanged)
- `execution_context.sql` — run context (unchanged)
- `audit_tracking.sql` — model & DQ audit persistence (unchanged)
- `load_strategies.sql` — all load SQL generators
- `load_executors.sql` — all six execution macros
- `pre_load_checks.sql` — shared pre-load, source volume threshold, TYPE2 pre-load checks
- `post_load_checks.sql` — TYPE1/TYPE2/INSERT/UPDATE/TRUNCATE_LOAD/DELETE_INSERT post-load checks
- `snowflake_dml.sql` — Snowflake DML executor (unchanged)
- `generate_schema_name.sql` — schema naming override (unchanged)
- `dev_probes.sql` — demo macros, not part of production dispatcher

## Safe installation (run from the dbt project root)

1. Ensure your source-controlled baseline is committed.
2. Rename the existing macro directory **outside** the configured macro path:
   `mv macros macros_before_consolidation`
3. Unzip `consolidated_macros.zip` from your Downloads directory into the dbt project root.
   Example: `unzip ~/Downloads/consolidated_macros.zip -d .`
4. Confirm there is one active macro directory and run:
   `dbt parse --no-partial-parse`
   `dbt ls --resource-type model --no-partial-parse`
5. If the new directory fails validation, restore: `mv macros macros_consolidation_failed && mv macros_before_consolidation macros`
6. Once validated, keep the previous directory outside `macro-paths` until you're satisfied.

Important: Never unzip this over the old `macros/` directory. The old definitions would coexist with the new definitions and cause duplicate-macro errors.

## Verification performed offline
- All original macro bodies byte-identical in their new groups
- Macro names count and uniqueness identical
- All non-materialization Jinja files parsed successfully with `do` support
- Dispatcher/materialization unchanged byte-for-byte
- ZIP integrity checked

Not verified: `dbt parse` against your Snowflake adapter, because dbt is not installed in the packaging environment. Your local dbt environment must perform that validation.

## Future enhancements (not in this package)
- Optional orchestration-driven date/timestamp processing windows
- Automatic source NOT NULL checks based on target column metadata
- Operational hardening and concurrency policy
