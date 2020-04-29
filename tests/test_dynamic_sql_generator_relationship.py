import pytest

from source.dynamic_sql_generator import DynamicStatementGenerator

NEWLINE = '\n'
TAB = '\t'
NEWLINE_AND_TAB = '\n\t'

TABLE_NAME = 'RELATIONSHIP'

STAGE_DATA_FIELDS = 'select parse_json($1):associateCode as ASSOCIATE_CODE, ' \
                    'parse_json($1):associateName as ASSOCIATE_NAME, parse_json($1):broadRegion ' \
                    'as BROAD_REGION_NAME, parse_json($1):broadRegionId as CRM_BROAD_REGION_ID, ' \
                    'parse_json($1):clientTier as CLIENT_TIER, parse_json($1):consultantRepCode ' \
                    'as CONSULTANT_REP_CD, parse_json($1):consultantRepName as ' \
                    'CONSULTANT_REP_NAME, parse_json($1):id as CRM_RELATIONSHIP_ID, ' \
                    'parse_json($1):marketSegment as MARKET_SEGMENT, parse_json($1):midRegion as ' \
                    'MID_REGION_NAME, parse_json($1):midRegionId as CRM_MID_REGION_ID, ' \
                    'parse_json($1):name as RELATIONSHIP_NAME, ' \
                    'parse_json($1):primaryConsultantCode as PRIMARY_CONSULTANT_CD, ' \
                    'parse_json($1):primaryConsultantCodeUUID as primary_consultant_id, ' \
                    'parse_json($1):primaryConsultantName as PRIMARY_CONSULTANT_NAME, ' \
                    'parse_json($1):region as REGION_NAME, parse_json($1):regionId as ' \
                    'CRM_REGION_ID, parse_json($1):relationshipType as RELATIONSHIP_TYPE, ' \
                    'parse_json($1):repOrRMCode as REPRESENTATIVE_OR_RM_CD, ' \
                    'parse_json($1):repOrRMName as REPRESENTATIVE_OR_RM_NAME, ' \
                    'parse_json($1):salesRepCode as SALES_REP_CD, parse_json($1):salesRepName as ' \
                    'SALES_REP_NAME, parse_json($1):secondaryConsultantCode as ' \
                    'SECONDARY_CONSULTANT_CD, parse_json($1):secondaryConsultantCodeUUID as ' \
                    'secondary_consultant_id, parse_json($1):secondaryConsultantName as ' \
                    'SECONDARY_CONSULTANT_NAME, parse_json($1):serviceRequirement as ' \
                    'SERVICE_REQUIREMENT, parse_json($1):servicedRelationship as ' \
                    'SERVICE_RELATIONSHIP, parse_json($1):team as TEAM, ' \
                    'parse_json($1):vehicleType as VEHICLE_TYPE ' \
                    'from @edw_stage_data/crm/relationship/relationship.json (file_format => json_format)'

IDENTIFIER_COL = 'RELATIONSHIP_ID'

COLUMN_NAMES = 'ASSOCIATE_CODE, ASSOCIATE_NAME, BROAD_REGION_NAME, CLIENT_TIER, CONSULTANT_REP_CD, CONSULTANT_REP_NAME, ' \
               'CRM_BROAD_REGION_ID, CRM_MID_REGION_ID, CRM_REGION_ID, CRM_RELATIONSHIP_ID, MARKET_SEGMENT, ' \
               'MID_REGION_NAME, PRIMARY_CONSULTANT_CD, PRIMARY_CONSULTANT_NAME, REGION_NAME, RELATIONSHIP_NAME, ' \
               'RELATIONSHIP_TYPE, REPRESENTATIVE_OR_RM_CD, REPRESENTATIVE_OR_RM_NAME, SALES_REP_CD, SALES_REP_NAME, ' \
               'SECONDARY_CONSULTANT_CD, SECONDARY_CONSULTANT_NAME, SERVICE_RELATIONSHIP, SERVICE_REQUIREMENT, TEAM, ' \
               'VEHICLE_TYPE'

COLUMN_NAMES_WITH_FOREIGN_COLS = 'SS.ASSOCIATE_CODE, ' \
                                 'SS.ASSOCIATE_NAME, ' \
                                 'SS.BROAD_REGION_NAME, ' \
                                 'SS.CLIENT_TIER, ' \
                                 'SS.CONSULTANT_REP_CD, ' \
                                 'SS.CONSULTANT_REP_NAME, ' \
                                 'SS.CRM_BROAD_REGION_ID, ' \
                                 'SS.CRM_MID_REGION_ID, ' \
                                 'SS.CRM_REGION_ID, ' \
                                 'SS.CRM_RELATIONSHIP_ID, ' \
                                 'SS.MARKET_SEGMENT, ' \
                                 'SS.MID_REGION_NAME, ' \
                                 'SS.PRIMARY_CONSULTANT_CD, ' \
                                 'SS.PRIMARY_CONSULTANT_NAME, ' \
                                 'SS.REGION_NAME, ' \
                                 'SS.RELATIONSHIP_NAME, ' \
                                 'SS.RELATIONSHIP_TYPE, ' \
                                 'SS.REPRESENTATIVE_OR_RM_CD, ' \
                                 'SS.REPRESENTATIVE_OR_RM_NAME, ' \
                                 'SS.SALES_REP_CD, ' \
                                 'SS.SALES_REP_NAME, ' \
                                 'SS.SECONDARY_CONSULTANT_CD, ' \
                                 'SS.SECONDARY_CONSULTANT_NAME, ' \
                                 'SS.SERVICE_RELATIONSHIP, ' \
                                 'SS.SERVICE_REQUIREMENT, ' \
                                 'SS.TEAM, ' \
                                 'SS.VEHICLE_TYPE, ' \
                                 'SS.primary_consultant_id, ' \
                                 'SS.secondary_consultant_id'

COLUMN_NAMES_WITH_ALIAS = 'S.ASSOCIATE_CODE, S.ASSOCIATE_NAME, S.BROAD_REGION_NAME, S.CLIENT_TIER, S.CONSULTANT_REP_CD, S.CONSULTANT_REP_NAME, ' \
                          'S.CRM_BROAD_REGION_ID, S.CRM_MID_REGION_ID, S.CRM_REGION_ID, S.CRM_RELATIONSHIP_ID, S.MARKET_SEGMENT, ' \
                          'S.MID_REGION_NAME, S.PRIMARY_CONSULTANT_CD, S.PRIMARY_CONSULTANT_NAME, S.REGION_NAME, S.RELATIONSHIP_NAME, ' \
                          'S.RELATIONSHIP_TYPE, S.REPRESENTATIVE_OR_RM_CD, S.REPRESENTATIVE_OR_RM_NAME, S.SALES_REP_CD, S.SALES_REP_NAME, ' \
                          'S.SECONDARY_CONSULTANT_CD, S.SECONDARY_CONSULTANT_NAME, S.SERVICE_RELATIONSHIP, S.SERVICE_REQUIREMENT, S.TEAM, ' \
                          'S.VEHICLE_TYPE'

FOREIGN_KEY_COLS = 'primary_consultant_id, secondary_consultant_id'

FOREIGN_KEY_COLS_WITH_ALIAS = 'S.primary_consultant_id, S.secondary_consultant_id'

FOREIGN_KEY_COLS_WITH_TABLE_ALIAS = 'CONSULTANT.consultant_id AS primary_consultant_id, CONSULTANT.consultant_id ' \
                                    'AS secondary_consultant_id'

TIMESTAMP_COLUMNS = 'INSERT_UTC_DATETIME, START_DATETIME, END_DATETIME'

HASH_KEY_COLS = 'select hash(' \
                'SS.ASSOCIATE_CODE, ' \
                'SS.ASSOCIATE_NAME, ' \
                'SS.BROAD_REGION_NAME, ' \
                'SS.CLIENT_TIER, ' \
                'SS.CONSULTANT_REP_CD, ' \
                'SS.CONSULTANT_REP_NAME, ' \
                'SS.CRM_RELATIONSHIP_ID, ' \
                'SS.MARKET_SEGMENT, ' \
                'SS.MID_REGION_NAME, ' \
                'SS.PRIMARY_CONSULTANT_CD, ' \
                'SS.PRIMARY_CONSULTANT_NAME, ' \
                'SS.REGION_NAME, ' \
                'SS.RELATIONSHIP_NAME, ' \
                'SS.RELATIONSHIP_TYPE, ' \
                'SS.REPRESENTATIVE_OR_RM_CD, ' \
                'SS.REPRESENTATIVE_OR_RM_NAME, ' \
                'SS.SALES_REP_CD, ' \
                'SS.SALES_REP_NAME, ' \
                'SS.SECONDARY_CONSULTANT_CD, ' \
                'SS.SECONDARY_CONSULTANT_NAME, ' \
                'SS.SERVICE_RELATIONSHIP, ' \
                'SS.SERVICE_REQUIREMENT, ' \
                'SS.TEAM, ' \
                'SS.VEHICLE_TYPE' \
                ') as source_key'

STAGE_COLS = '(select parse_json($1):associateCode as ASSOCIATE_CODE, ' \
             'parse_json($1):associateName as ASSOCIATE_NAME, parse_json($1):broadRegion ' \
             'as BROAD_REGION_NAME, parse_json($1):broadRegionId as CRM_BROAD_REGION_ID, ' \
             'parse_json($1):clientTier as CLIENT_TIER, parse_json($1):consultantRepCode ' \
             'as CONSULTANT_REP_CD, parse_json($1):consultantRepName as ' \
             'CONSULTANT_REP_NAME, parse_json($1):id as CRM_RELATIONSHIP_ID, ' \
             'parse_json($1):marketSegment as MARKET_SEGMENT, parse_json($1):midRegion as ' \
             'MID_REGION_NAME, parse_json($1):midRegionId as CRM_MID_REGION_ID, ' \
             'parse_json($1):name as RELATIONSHIP_NAME, ' \
             'parse_json($1):primaryConsultantCode as PRIMARY_CONSULTANT_CD, ' \
             'parse_json($1):primaryConsultantCodeUUID as primary_consultant_id, ' \
             'parse_json($1):primaryConsultantName as PRIMARY_CONSULTANT_NAME, ' \
             'parse_json($1):region as REGION_NAME, parse_json($1):regionId as ' \
             'CRM_REGION_ID, parse_json($1):relationshipType as RELATIONSHIP_TYPE, ' \
             'parse_json($1):repOrRMCode as REPRESENTATIVE_OR_RM_CD, ' \
             'parse_json($1):repOrRMName as REPRESENTATIVE_OR_RM_NAME, ' \
             'parse_json($1):salesRepCode as SALES_REP_CD, parse_json($1):salesRepName as ' \
             'SALES_REP_NAME, parse_json($1):secondaryConsultantCode as ' \
             'SECONDARY_CONSULTANT_CD, parse_json($1):secondaryConsultantCodeUUID as ' \
             'secondary_consultant_id, parse_json($1):secondaryConsultantName as ' \
             'SECONDARY_CONSULTANT_NAME, parse_json($1):serviceRequirement as ' \
             'SERVICE_REQUIREMENT, parse_json($1):servicedRelationship as ' \
             'SERVICE_RELATIONSHIP, parse_json($1):team as TEAM, ' \
             'parse_json($1):vehicleType as VEHICLE_TYPE ' \
             'from @edw_stage_data/crm/relationship/relationship.json (file_format => json_format)) SS'

JOIN_CONDITIONS = 'LEFT JOIN CONSULTANT AS CONSULTANT ON ' \
                  'CONSULTANT.crm_consultant_id = trim(SS.primary_consultant_id) AND ' \
                  "CONSULTANT.END_DATETIME = to_date('9999-12-31') LEFT JOIN " \
                  'CONSULTANT AS CONSULTANT1 ON ' \
                  'CONSULTANT1.crm_consultant_id = trim(SS.secondary_consultant_id) AND ' \
                  'CONSULTANT1.END_DATETIME = to_date(\'9999-12-31\')) AS SRC'


# Fixtures
@pytest.fixture
def dynamic_statement_generator():
    return DynamicStatementGenerator('@edw_stage_data/crm/relationship/relationship.json',
                                     'relationship', True)


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
    verify_answer('{}, {}, {}, {}'.format(IDENTIFIER_COL, COLUMN_NAMES, FOREIGN_KEY_COLS, TIMESTAMP_COLUMNS),
                  ', '.join(answer))


def test_generate_insert_into_statement_column_names_clause(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_into_statement_column_names_clause()
    verify_answer('INSERT into {} ({}, {}, {}, {})'.format(TABLE_NAME, IDENTIFIER_COL,
                                                           COLUMN_NAMES,
                                                           FOREIGN_KEY_COLS,
                                                           TIMESTAMP_COLUMNS),
                  answer)


def test_generate_select_statement_with_column_alias_and_datetime_columns(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_statement_with_attributes()
    verify_answer('SELECT S.source_key, {}, {}, '
                  'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), '
                  'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), '
                  'to_date(\'9999-12-31\') from'
                  .format(COLUMN_NAMES_WITH_ALIAS, FOREIGN_KEY_COLS_WITH_ALIAS), answer)


def test_generate_select_statement_without_column_alias_and_datetime_columns(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_statement_with_attributes(False, False)
    verify_answer('SELECT source_key, {}, {} from'.format(COLUMN_NAMES, FOREIGN_KEY_COLS), answer)


def test_generate_select_with_hash_table_and_foreign_table_cols(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_with_hashkey_and_foreign_table_cols()
    verify_answer(
        '{}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_FOREIGN_COLS), answer)


def test_generate_select_columns_from_stage(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_using_stage_columns()
    verify_answer('({}) SS'.format(STAGE_DATA_FIELDS), answer)


def test_insert_into_statement_select_clause(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_into_statement_select_clause()
    select_with_column_alias_and_current_time = 'SELECT S.source_key, {0}, {1}, ' \
                                                'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_date(\'9999-12-31\')' \
        .format(COLUMN_NAMES_WITH_ALIAS, FOREIGN_KEY_COLS_WITH_ALIAS)
    select_with_cols_and_foreign_keys = 'SELECT source_key, {}, {} from'.format(COLUMN_NAMES, FOREIGN_KEY_COLS)
    select_with_hash_cols_and_foreign_key_cols = '{}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_FOREIGN_COLS)
    select_with_stage_data = '{}'.format(STAGE_COLS)
    join_conditions = '{}){newline_and_tab}AS S'.format(JOIN_CONDITIONS, newline_and_tab=NEWLINE_AND_TAB)
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
    verify_answer('LEFT JOIN RELATIONSHIP T ON T.RELATIONSHIP_ID '
                  '= S.source_key WHERE T.RELATIONSHIP_ID is null', answer)


def test_generate_select_from_stage(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_from_stage()
    allcols_including_foreign_and_hash = '{}, {} from'.format(HASH_KEY_COLS,
                                                              COLUMN_NAMES_WITH_FOREIGN_COLS)
    expected_response = '{newline}{tab}{tab}({0}{newline}{tab}{tab}{1}{newline}{tab}{tab}{2}'. \
        format(allcols_including_foreign_and_hash, STAGE_COLS, JOIN_CONDITIONS,
               newline_and_tab=NEWLINE_AND_TAB,
               newline=NEWLINE,
               tab=TAB)
    verify_answer(answer, expected_response)


def test_generate_insert_statement_with_all_conditions(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_statement_with_all_conditions()
    insert_statement = insert_statement_generator()
    verify_answer(insert_statement, answer)


def insert_statement_generator():
    insert_cols = 'INSERT into {} ({}, {}, {}, {})'.format(TABLE_NAME, IDENTIFIER_COL, COLUMN_NAMES, FOREIGN_KEY_COLS,
                                                           TIMESTAMP_COLUMNS)
    select_with_column_alias_and_current_time = 'SELECT S.source_key, {}, {}, ' \
                                                'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_date(\'9999-12-31\')' \
        .format(COLUMN_NAMES_WITH_ALIAS, FOREIGN_KEY_COLS_WITH_ALIAS)
    select_with_cols_and_foreign_keys = 'SELECT source_key, {}, {} from\t'.format(COLUMN_NAMES, FOREIGN_KEY_COLS)
    select_with_hash_cols_and_foreign_key_cols = '({}, {} from'.format(HASH_KEY_COLS,
                                                                       COLUMN_NAMES_WITH_FOREIGN_COLS)
    select_with_stage_data = '{}'.format(STAGE_COLS)
    join_conditions = JOIN_CONDITIONS
    primary_key_null_condition = 'AS S{newline}LEFT JOIN RELATIONSHIP T ' \
                                 'ON T.RELATIONSHIP_ID = S.source_key WHERE T.RELATIONSHIP_ID is null'. \
        format(newline_and_tab=NEWLINE_AND_TAB, newline=NEWLINE, tab=TAB)
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
    return 'UPDATE RELATIONSHIP set END_DATETIME = ' \
           'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))) ' \
           'where RELATIONSHIP_ID ' \
           'not in {newline_and_tab}({0}{newline}{tab}' \
           'from{newline_and_tab}{tab}{1}{newline_and_tab})and END_DATETIME = to_date(\'9999-12-31\')'.format(
        HASH_KEY_COLS, STAGE_COLS,
        newline_and_tab=NEWLINE_AND_TAB, newline=NEWLINE, tab=TAB)


def test_generate_insert_and_update_statements(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_and_update_statements()
    verify_answer(insert_statement_generator(), answer.get("INSERT_STATEMENT"))
    verify_answer(update_statement_generator(), answer.get("UPDATE_STATEMENT"))
