import pytest

from source.dynamic_sql_generator import DynamicStatementGenerator

NEWLINE = '\n'
TAB = '\t'
NEWLINE_AND_TAB = '\n\t'

TABLE_NAME = 'MID_REGION'

STAGE_DATA_FIELDS = 'select ' \
                    'parse_json($1):uuid as CRM_MID_REGION_ID, ' \
                    'parse_json($1):value as MID_REGION_NAME ' \
                    'from @edw_stage_data/mid-region.json (file_format => json_format)'

IDENTIFIER_COL = 'MID_REGION_ID'

COLUMN_NAMES = 'CRM_MID_REGION_ID, MID_REGION_NAME'

COLUMN_NAMES_WITH_SS = 'SS.CRM_MID_REGION_ID, SS.MID_REGION_NAME'

COLUMN_NAMES_WITH_VALUE = 'CRM_MID_REGION_ID, MID_REGION_NAME'

COLUMN_NAMES_WITH_ALIAS = 'S.CRM_MID_REGION_ID, S.MID_REGION_NAME'

FOREIGN_KEY_COLS = ''

TIMESTAMP_COLUMNS = 'INSERT_UTC_DATETIME, START_DATETIME, END_DATETIME'

HASH_KEY_COLS = 'select hash(SS.CRM_MID_REGION_ID, SS.MID_REGION_NAME) as source_key'

STAGE_COLS = '(' + STAGE_DATA_FIELDS + ') SS'

JOIN_CONDITIONS_NO_FOREIGN_KEY = ') AS SRC'


# Fixtures
@pytest.fixture
def dynamic_statement_generator():
    return DynamicStatementGenerator('@edw_stage_data/mid-region.json', 'mid-region', True)


# Helpers
def verify_answer(expected, answer):
    assert expected == answer


# Test Cases
def test_read_bad_file():
    instance = None
    try:
        instance = DynamicStatementGenerator('This is ignored so remove this param', 'BadFeedName', True)
    except FileNotFoundError as e:
        verify_answer('No such file or directory', e.strerror)
        verify_answer('./data/BadFeedName-metadata.json', e.filename)
    verify_answer(None, instance)


def test_ordered_dictionary_key_order(dynamic_statement_generator):
    answer = dynamic_statement_generator.get_timestamp_column_names()
    verify_answer(['INSERT_UTC_DATETIME', 'START_DATETIME', 'END_DATETIME'], answer)


def test_get_column_names(dynamic_statement_generator):
    answer = dynamic_statement_generator.get_column_names()
    verify_answer('{}, {}, {}'.format(IDENTIFIER_COL, COLUMN_NAMES, TIMESTAMP_COLUMNS),
                  ', '.join(answer))


def test_generate_insert_into_statement_column_names_clause(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_into_statement_column_names_clause()
    verify_answer(
        'INSERT into {} ({}, {}, {})'.format(TABLE_NAME,
                                             IDENTIFIER_COL,
                                             COLUMN_NAMES,
                                             TIMESTAMP_COLUMNS), answer)


def test_generate_select_statement_with_column_alias_and_datetime_columns(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_statement_with_attributes()
    verify_answer('SELECT S.source_key, {}, '
                  'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), '
                  'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), '
                  'to_date(\'9999-12-31\') from'
                  .format(COLUMN_NAMES_WITH_ALIAS), answer)


def test_generate_select_statement_without_column_alias_and_datetime_columns(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_statement_with_attributes(False, False)
    verify_answer('SELECT source_key, {} from'.format(COLUMN_NAMES_WITH_VALUE), answer)


def test_generate_select_with_hash_table_and_foreign_table_cols(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_with_hashkey_and_foreign_table_cols()
    verify_answer('{}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_SS), answer)


def test_generate_select_columns_from_stage(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_using_stage_columns()
    verify_answer('({}) SS'.format(STAGE_DATA_FIELDS), answer)


def test_insert_into_statement_select_clause(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_into_statement_select_clause()
    select_with_column_alias_and_current_time = 'SELECT S.source_key, {0}, ' \
                                                'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_date(\'9999-12-31\')' \
        .format(COLUMN_NAMES_WITH_ALIAS)
    select_with_cols_and_foreign_keys = 'SELECT source_key, {} from'.format(COLUMN_NAMES_WITH_VALUE)
    select_with_hash_cols_and_foreign_key_cols = '{}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_SS)
    select_with_stage_data = '{}'.format(STAGE_COLS)
    join_conditions = '{}){newline_and_tab}AS S'.format(JOIN_CONDITIONS_NO_FOREIGN_KEY, newline_and_tab=NEWLINE_AND_TAB)
    verify_answer(
        '{0} from{newline_and_tab}{tab}({1}{tab}{newline_and_tab}{tab}({2}{newline_and_tab}{tab}{3}{newline_and_tab}{tab}{4}'.format(
            select_with_column_alias_and_current_time,
            select_with_cols_and_foreign_keys,
            select_with_hash_cols_and_foreign_key_cols,
            select_with_stage_data,
            join_conditions,
            newline_and_tab=NEWLINE_AND_TAB,
            newline=NEWLINE,
            tab=TAB), answer)


def test_generate_join_conditions(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_source_key_outer_join_conditions()
    verify_answer('LEFT JOIN {table_name} T ON T.{identifier_col} = '
                  'S.source_key WHERE T.{identifier_col} is null'.format(table_name=TABLE_NAME,
                                                                         identifier_col=IDENTIFIER_COL), answer)


def test_generate_select_from_stage(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_from_stage()
    allcols_including_foreign_and_hash = '{}, {} from'.format(HASH_KEY_COLS,
                                                              COLUMN_NAMES_WITH_SS)
    expected_response = '{newline}{tab}{tab}({0}{newline}{tab}{tab}{1}{newline}{tab}{tab}{2}'. \
        format(allcols_including_foreign_and_hash, STAGE_COLS, JOIN_CONDITIONS_NO_FOREIGN_KEY,
               newline_and_tab=NEWLINE_AND_TAB,
               newline=NEWLINE,
               tab=TAB)
    verify_answer(answer, expected_response)


def test_generate_insert_statement_with_all_conditions(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_statement_with_all_conditions()
    insert_statement = insert_statement_generator()
    verify_answer(insert_statement, answer)


def insert_statement_generator():
    insert_cols = 'INSERT into {table_name} ({identifier_col}, {col_names}, {timestamp_cols})'.format(
        identifier_col=IDENTIFIER_COL,
        table_name=TABLE_NAME,
        col_names=COLUMN_NAMES,
        timestamp_cols=TIMESTAMP_COLUMNS)
    select_with_column_alias_and_current_time = 'SELECT S.source_key, {}, ' \
                                                'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_date(\'9999-12-31\')' \
        .format(COLUMN_NAMES_WITH_ALIAS)
    select_with_cols_and_foreign_keys = 'SELECT source_key, {} from\t'.format(COLUMN_NAMES_WITH_VALUE)
    select_with_hash_cols_and_foreign_key_cols = '({}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_SS)
    select_with_stage_data = '{}'.format(STAGE_COLS)
    join_conditions = JOIN_CONDITIONS_NO_FOREIGN_KEY
    primary_key_null_condition = 'AS S{newline}LEFT JOIN {table_name} T ' \
                                 'ON T.{identifier_col} = S.source_key WHERE T.{identifier_col} is null'. \
        format(table_name=TABLE_NAME,
               identifier_col=IDENTIFIER_COL,
               newline_and_tab=NEWLINE_AND_TAB,
               newline=NEWLINE,
               tab=TAB)
    insert_statement = '{0}{newline_and_tab}{1} from{newline_and_tab}{tab}({2}{newline_and_tab}{tab}{3}' \
                       '{newline_and_tab}{tab}{4}{newline_and_tab}{tab}{5}){newline_and_tab}{6}'.format(
        insert_cols, select_with_column_alias_and_current_time, select_with_cols_and_foreign_keys,
        select_with_hash_cols_and_foreign_key_cols, select_with_stage_data, join_conditions, primary_key_null_condition,
        newline_and_tab=NEWLINE_AND_TAB, newline=NEWLINE, tab=TAB)
    return insert_statement


def test_generate_update_statement(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_update_statement()
    response = update_statement_generator()
    verify_answer(answer, response)


def update_statement_generator():
    return 'UPDATE {0} set END_DATETIME = ' \
           'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))) ' \
           'where {1} ' \
           'not in {newline_and_tab}({2}{newline}{tab}' \
           'from{newline_and_tab}{tab}{3}{newline_and_tab})and END_DATETIME = to_date(\'9999-12-31\')'.format(
        TABLE_NAME, IDENTIFIER_COL, HASH_KEY_COLS, STAGE_COLS,
        newline_and_tab=NEWLINE_AND_TAB, newline=NEWLINE, tab=TAB)


def test_generate_insert_and_update_statements(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_and_update_statements()
    verify_answer(insert_statement_generator(), answer.get("INSERT_STATEMENT"))
    verify_answer(update_statement_generator(), answer.get("UPDATE_STATEMENT"))
