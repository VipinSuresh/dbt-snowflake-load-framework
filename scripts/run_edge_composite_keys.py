import json
import re
import sys
from pathlib import Path

from run_regression_update import run, latest_invocation_id

PROJECT = Path(__file__).resolve().parents[1]
MODEL_NAMES = [
    'regression_customer_type1',
    'regression_customer_type2',
    'regression_customer_insert',
    'regression_customer_update',
    'regression_customer_delete_insert',
    'regression_customer_replace',
]

SINGLE_KEY_RE = re.compile(
    r'(?m)^(?P<indent>\s*)natural_keys:\s*\n(?P=indent)  - CUSTOMER_ID\s*$'
)
COMPOSITE_KEY_RE = re.compile(
    r'(?m)^(?P<indent>\s*)natural_keys:\s*\n(?P=indent)  - CUSTOMER_ID\s*\n(?P=indent)  - CITY\s*$'
)
BAD_JINJA_RE = re.compile(
    r"(?m)^(?P<indent>\s*)natural_keys:\s*\{\{\s*\['CUSTOMER_ID',\s*'CITY'\]\s*if\s*var\('regression_composite_keys',\s*false\)\s*else\s*\['CUSTOMER_ID'\]\s*\}\}\s*$"
)

STRATEGIES = [
    {
        'name': 'TYPE1',
        'model': 'regression_customer_type1',
        'target': 'DIM_CUSTOMER_TYPE1_TEST',
        'reset': 'regression_reset_type1',
        'state': 'regression_composite_type1_state',
        'positive_dml': 'MERGE COMPLETE | Inserted=1 | Updated=1 | Deleted=0',
        'null_check': 'SOURCE_NULL_KEYS',
        'dup_check': 'SOURCE_DUPLICATE_KEYS',
    },
    {
        'name': 'INSERT',
        'model': 'regression_customer_insert',
        'target': 'DIM_CUSTOMER_INSERT_TEST',
        'reset': 'regression_reset_insert',
        'state': 'regression_composite_insert_state',
        'positive_dml': 'MERGE COMPLETE | Inserted=1 | Updated=0 | Deleted=0',
        'null_check': 'SOURCE_NULL_KEYS',
        'dup_check': 'SOURCE_DUPLICATE_KEYS',
    },
    {
        'name': 'UPDATE',
        'model': 'regression_customer_update',
        'target': 'DIM_CUSTOMER_UPDATE_TEST',
        'reset': 'regression_reset_update',
        'state': 'regression_composite_update_state',
        'positive_dml': 'MERGE COMPLETE | Inserted=0 | Updated=1 | Deleted=0',
        'null_check': 'SOURCE_NULL_KEYS',
        'dup_check': 'SOURCE_DUPLICATE_KEYS',
    },
    {
        'name': 'DELETE_INSERT',
        'model': 'regression_customer_delete_insert',
        'target': 'DIM_CUSTOMER_DELETE_INSERT_TEST',
        'reset': 'regression_reset_delete_insert',
        'state': 'regression_composite_delete_insert_state',
        'positive_dml': 'DELETE_INSERT DML COMMITTED | Deleted=2 | Inserted=3',
        'null_check': 'SOURCE_NULL_KEYS',
        'dup_check': 'SOURCE_DUPLICATE_KEYS',
    },
    {
        'name': 'TRUNCATE_LOAD',
        'model': 'regression_customer_replace',
        'target': 'DIM_CUSTOMER_REPLACE_TEST',
        'reset': 'regression_reset_replace',
        'state': 'regression_composite_replace_state',
        'positive_dml': 'TRUNCATE_LOAD DML COMMITTED | Deleted=3 | Inserted=3',
        'null_check': 'SOURCE_NULL_KEYS',
        'dup_check': 'SOURCE_DUPLICATE_KEYS',
    },
]


def vars_json(case):
    return json.dumps({'regression_case': case})


def yaml_paths():
    return [PROJECT / 'models' / 'regression' / f'{name}.yml' for name in MODEL_NAMES]


def normalize_invalid_yaml(text: str) -> str:
    match = BAD_JINJA_RE.search(text)
    if not match:
        return text
    indent = match.group('indent')
    return BAD_JINJA_RE.sub(
        f'{indent}natural_keys:\n{indent}  - CUSTOMER_ID', text, count=1
    )


def enable_composite_keys():
    originals = {}
    for path in yaml_paths():
        if not path.exists():
            raise RuntimeError(f'Missing regression YAML: {path}')
        original = path.read_text()
        originals[path] = original
        text = normalize_invalid_yaml(original)

        if COMPOSITE_KEY_RE.search(text):
            path.write_text(text)
            continue

        match = SINGLE_KEY_RE.search(text)
        if not match:
            raise RuntimeError(
                f'Could not locate single natural_keys block in {path}. '
                'Run scripts/repair_composite_key_yaml.py first.'
            )
        indent = match.group('indent')
        replacement = (
            f'{indent}natural_keys:\n'
            f'{indent}  - CUSTOMER_ID\n'
            f'{indent}  - CITY'
        )
        path.write_text(SINGLE_KEY_RE.sub(replacement, text, count=1))
        print(f'COMPOSITE KEY ENABLED: {path.relative_to(PROJECT)}')
    return originals


def restore_yamls(originals):
    for path, original in originals.items():
        # Never restore the known-invalid Jinja line. If this run started from that
        # state, normalize it back to the valid single-key config instead.
        path.write_text(normalize_invalid_yaml(original))
    print('REGRESSION YAML RESTORED TO SINGLE-KEY CONFIGURATION')


def verify_failure(strategy, case, check_name):
    output = run([
        'run', '--select', strategy['model'],
        '--vars', vars_json(case)
    ], should_succeed=False)

    if f'{check_name} | FAIL' not in output:
        raise RuntimeError(f"{strategy['name']}: expected {check_name} failure missing")

    forbidden = [
        'MERGE COMPLETE', 'TYPE2 DML COMMITTED',
        'DELETE_INSERT DML COMMITTED', 'TRUNCATE_LOAD DML COMMITTED'
    ]
    if any(token in output for token in forbidden):
        raise RuntimeError(f"{strategy['name']}: business DML executed during pre-load key failure")

    failed_invocation = latest_invocation_id()
    run([
        'run-operation', 'regression_verify_preload_key_failure',
        '--args', json.dumps({
            'invocation_id': failed_invocation,
            'model_name': strategy['model'],
            'target_identifier': strategy['target'],
            'check_name': check_name,
        })
    ])

    run(['test', '--select', strategy['state']])


def run_standard_strategy(strategy):
    print('\n' + '=' * 68)
    print(f"COMPOSITE KEY EDGE CASE: {strategy['name']}")
    print('=' * 68)

    run(['run-operation', strategy['reset']])

    output = run([
        'run', '--select', strategy['model'],
        '--vars', vars_json('composite_positive')
    ])
    if strategy['positive_dml'] not in output:
        raise RuntimeError(f"{strategy['name']}: composite-key DML counts unexpected")

    run(['test', '--select', strategy['state']])
    print(f"PASS: {strategy['name']} accepts duplicate first key when full composite key differs")

    verify_failure(strategy, 'composite_null_key', strategy['null_check'])
    print(f"PASS: {strategy['name']} rejects NULL composite-key component and preserves target")

    verify_failure(strategy, 'composite_duplicate_key', strategy['dup_check'])
    print(f"PASS: {strategy['name']} rejects duplicate full composite key and preserves target")


def run_type2():
    strategy = {
        'name': 'TYPE2',
        'model': 'regression_customer_type2',
        'target': 'DIM_CUSTOMER_TYPE2_TEST',
        'reset': 'regression_reset_type2',
        'state': 'regression_composite_type2_changed',
        'null_check': 'TYPE2_SOURCE_NULL_KEYS',
        'dup_check': 'TYPE2_SOURCE_DUPLICATES',
    }

    print('\n' + '=' * 68)
    print('COMPOSITE KEY EDGE CASE: TYPE2')
    print('=' * 68)
    run(['run-operation', strategy['reset']])

    output = run([
        'run', '--select', strategy['model'],
        '--vars', vars_json('composite_positive')
    ])
    if 'TYPE2 DML COMMITTED | inserted versions=3 | expired versions=0' not in output:
        raise RuntimeError('TYPE2: composite initial history counts unexpected')
    run(['test', '--select', 'regression_composite_type2_initial'])
    print('PASS: TYPE2 stores two CUSTOMER_ID=101 rows as distinct Dallas/Austin composite keys')

    output = run([
        'run', '--select', strategy['model'],
        '--vars', vars_json('composite_changed')
    ])
    if 'TYPE2 DML COMMITTED | inserted versions=1 | expired versions=1' not in output:
        raise RuntimeError('TYPE2: composite changed-history counts unexpected')
    run(['test', '--select', strategy['state']])
    print('PASS: TYPE2 versions only the changed Dallas composite key; Austin remains unchanged')

    verify_failure(strategy, 'composite_null_key', strategy['null_check'])
    print('PASS: TYPE2 rejects NULL composite-key component and preserves history')

    verify_failure(strategy, 'composite_duplicate_key', strategy['dup_check'])
    print('PASS: TYPE2 rejects duplicate full composite key and preserves history')


def main():
    print('COMPOSITE NATURAL-KEY EDGE-CASE REGRESSION')
    print('Natural key under test: CUSTOMER_ID + CITY')
    print('Positive case intentionally repeats CUSTOMER_ID with different CITY values.\n')

    originals = enable_composite_keys()
    try:
        run_standard_strategy(STRATEGIES[0])
        run_type2()
        for strategy in STRATEGIES[1:]:
            run_standard_strategy(strategy)
        print('\nALL SIX COMPOSITE-KEY EDGE-CASE TESTS PASSED')
    finally:
        restore_yamls(originals)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'\nCOMPOSITE KEY EDGE-CASE REGRESSION FAILED: {exc}')
        sys.exit(1)
