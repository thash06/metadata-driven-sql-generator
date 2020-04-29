import pytest

from source.dynamic_sql_generator import DynamicStatementGenerator

NEWLINE = '\n'
TAB = '\t'
NEWLINE_AND_TAB = '\n\t'

TABLE_NAME = 'OPPORTUNITY'

STAGE_DATA_FIELDS = 'select ' \
                    'parse_json($1):awardDate as AWARD_DATE, ' \
                    'parse_json($1):capacityAmount as CAPACITY_AMOUNT, ' \
                    'parse_json($1):capacityNotes as CAPACITY_NOTES, ' \
                    'parse_json($1):capacityRemovedDate as CAPACITY_REMOVED_DATE, ' \
                    'parse_json($1):capacityTracking as CAPACITY_TRACKING, ' \
                    'parse_json($1):capacityTrackingClosed as CAPACITY_TRACKING_CLOSED, ' \
                    'parse_json($1):capacityTrackingStrategy as CAPACITY_TRACKING_STRATEGY, ' \
                    'parse_json($1):closeDate as OPPORTUNITY_CLOSE_DATE, ' \
                    'parse_json($1):closed as IS_OPPORTUNITY_CLOSED, ' \
                    'parse_json($1):consultantRepCode as CONSULTANT_REP_CD, ' \
                    'parse_json($1):contactUUID as CONTACT_ID, ' \
                    'parse_json($1):countryUUID as COUNTRY_ID, ' \
                    'parse_json($1):currencyCodeUUID as CURRENCY_ID, ' \
                    'parse_json($1):currentStatus as OPPORTUNITY_CURRENT_STATUS, ' \
                    'parse_json($1):enterDate as OPPORTUNITY_ENTER_DATE, ' \
                    'parse_json($1):estFundingDate as ESTIMATED_FUNDING_DATE, ' \
                    'parse_json($1):estimatedRevenue as ESTIMATED_REVENUE, ' \
                    'parse_json($1):feeEstimate as ESTIMATED_FEE, ' \
                    'parse_json($1):notes as NOTES, ' \
                    'parse_json($1):opportunityId as CRM_OPPORTUNITY_ID, ' \
                    'parse_json($1):opportunityType as OPPORTUNITY_TYPE, ' \
                    'parse_json($1):origination as ORIGINATION, ' \
                    'parse_json($1):portfolioCode as PORTFOLIO_CODE, ' \
                    'parse_json($1):primaryConsultantCode as PRIMARY_CONSULTANT_CD, ' \
                    'parse_json($1):primaryConsultantCodeUUID as primary_consultant_id, ' \
                    'parse_json($1):primaryContact as PRIMARY_CONTACT, ' \
                    'parse_json($1):probability as PROBABILITY, ' \
                    'parse_json($1):relationshipId as CRM_RELATIONSHIP_ID, ' \
                    'parse_json($1):relationshipType as OPPORTUNITY_RELATIONSHIP_TYPE, ' \
                    'parse_json($1):relationshipUUID as RELATIONSHIP_ID, ' \
                    'parse_json($1):repCode as REPRESENTATIVE_CD, ' \
                    'parse_json($1):salesPhase as SALES_PHASE, ' \
                    'parse_json($1):secondaryConsultantCode as SECONDARY_CONSULTANT_CD, ' \
                    'parse_json($1):secondaryConsultantCodeUUID as secondary_consultant_id, ' \
                    'parse_json($1):strategyUUID as STRATEGY_ID, ' \
                    'parse_json($1):targetAssets as TARGET_ASSETS, ' \
                    'parse_json($1):territory as TERRITORY ' \
                    'from @edw_stage_data/crm/opportunity/opportunity.json (file_format => json_format)'

IDENTIFIER_COL = 'OPPORTUNITY_ID'

COLUMN_NAMES = 'AWARD_DATE, ' \
               'CAPACITY_AMOUNT, ' \
               'CAPACITY_NOTES, ' \
               'CAPACITY_REMOVED_DATE, ' \
               'CAPACITY_TRACKING, ' \
               'CAPACITY_TRACKING_CLOSED, ' \
               'CAPACITY_TRACKING_STRATEGY, ' \
               'CONSULTANT_REP_CD, ' \
               'CONTACT_ID, ' \
               'COUNTRY_ID, ' \
               'CRM_OPPORTUNITY_ID, ' \
               'CRM_RELATIONSHIP_ID, ' \
               'CURRENCY_ID, ' \
               'ESTIMATED_FEE, ' \
               'ESTIMATED_FUNDING_DATE, ' \
               'ESTIMATED_REVENUE, ' \
               'IS_OPPORTUNITY_CLOSED, ' \
               'NOTES, ' \
               'OPPORTUNITY_CLOSE_DATE, ' \
               'OPPORTUNITY_CURRENT_STATUS, ' \
               'OPPORTUNITY_ENTER_DATE, ' \
               'OPPORTUNITY_RELATIONSHIP_TYPE, ' \
               'OPPORTUNITY_TYPE, ' \
               'ORIGINATION, ' \
               'PORTFOLIO_CODE, ' \
               'PRIMARY_CONSULTANT_CD, ' \
               'PRIMARY_CONTACT, ' \
               'PROBABILITY, ' \
               'RELATIONSHIP_ID, ' \
               'REPRESENTATIVE_CD, ' \
               'SALES_PHASE, ' \
               'SECONDARY_CONSULTANT_CD, ' \
               'STRATEGY_ID, ' \
               'TARGET_ASSETS, ' \
               'TERRITORY, ' \
               'primary_consultant_id, secondary_consultant_id'

COLUMN_NAMES_WITH_FOREIGN_COLS = 'CONSULTANT.consultant_id AS primary_consultant_id, ' \
                                 'CONSULTANT.consultant_id AS secondary_consultant_id, ' \
                                 'CONTACT.contact_id AS CONTACT_ID, ' \
                                 'COUNTRY.country_id AS COUNTRY_ID, ' \
                                 'CURRENCY.currency_id AS CURRENCY_ID, ' \
                                 'RELATIONSHIP.relationship_id AS RELATIONSHIP_ID, ' \
                                 'SS.AWARD_DATE, ' \
                                 'SS.CAPACITY_AMOUNT, ' \
                                 'SS.CAPACITY_NOTES, ' \
                                 'SS.CAPACITY_REMOVED_DATE, ' \
                                 'SS.CAPACITY_TRACKING, ' \
                                 'SS.CAPACITY_TRACKING_CLOSED, ' \
                                 'SS.CAPACITY_TRACKING_STRATEGY, ' \
                                 'SS.CONSULTANT_REP_CD, ' \
                                 'SS.CRM_OPPORTUNITY_ID, ' \
                                 'SS.CRM_RELATIONSHIP_ID, ' \
                                 'SS.ESTIMATED_FEE, ' \
                                 'SS.ESTIMATED_FUNDING_DATE, ' \
                                 'SS.ESTIMATED_REVENUE, ' \
                                 'SS.IS_OPPORTUNITY_CLOSED, ' \
                                 'SS.NOTES, ' \
                                 'SS.OPPORTUNITY_CLOSE_DATE, ' \
                                 'SS.OPPORTUNITY_CURRENT_STATUS, ' \
                                 'SS.OPPORTUNITY_ENTER_DATE, ' \
                                 'SS.OPPORTUNITY_RELATIONSHIP_TYPE, ' \
                                 'SS.OPPORTUNITY_TYPE, ' \
                                 'SS.ORIGINATION, ' \
                                 'SS.PORTFOLIO_CODE, ' \
                                 'SS.PRIMARY_CONSULTANT_CD, ' \
                                 'SS.PRIMARY_CONTACT, ' \
                                 'SS.PROBABILITY, ' \
                                 'SS.REPRESENTATIVE_CD, ' \
                                 'SS.SALES_PHASE, ' \
                                 'SS.SECONDARY_CONSULTANT_CD, ' \
                                 'SS.TARGET_ASSETS, ' \
                                 'SS.TERRITORY, ' \
                                 'STRATEGY.strategy_id AS STRATEGY_ID'

COLUMN_NAMES_WITH_ALIAS = 'S.AWARD_DATE, ' \
                          'S.CAPACITY_AMOUNT, ' \
                          'S.CAPACITY_NOTES, ' \
                          'S.CAPACITY_REMOVED_DATE, ' \
                          'S.CAPACITY_TRACKING, ' \
                          'S.CAPACITY_TRACKING_CLOSED, ' \
                          'S.CAPACITY_TRACKING_STRATEGY, ' \
                          'S.CONSULTANT_REP_CD, ' \
                          'S.CONTACT_ID, ' \
                          'S.COUNTRY_ID, ' \
                          'S.CRM_OPPORTUNITY_ID, ' \
                          'S.CRM_RELATIONSHIP_ID, ' \
                          'S.CURRENCY_ID, ' \
                          'S.ESTIMATED_FEE, ' \
                          'S.ESTIMATED_FUNDING_DATE, ' \
                          'S.ESTIMATED_REVENUE, ' \
                          'S.IS_OPPORTUNITY_CLOSED, ' \
                          'S.NOTES, ' \
                          'S.OPPORTUNITY_CLOSE_DATE, ' \
                          'S.OPPORTUNITY_CURRENT_STATUS, ' \
                          'S.OPPORTUNITY_ENTER_DATE, ' \
                          'S.OPPORTUNITY_RELATIONSHIP_TYPE, ' \
                          'S.OPPORTUNITY_TYPE, ' \
                          'S.ORIGINATION, ' \
                          'S.PORTFOLIO_CODE, ' \
                          'S.PRIMARY_CONSULTANT_CD, ' \
                          'S.PRIMARY_CONTACT, ' \
                          'S.PROBABILITY, ' \
                          'S.RELATIONSHIP_ID, ' \
                          'S.REPRESENTATIVE_CD, ' \
                          'S.SALES_PHASE, ' \
                          'S.SECONDARY_CONSULTANT_CD, ' \
                          'S.STRATEGY_ID, ' \
                          'S.TARGET_ASSETS, ' \
                          'S.TERRITORY, ' \
                          'S.primary_consultant_id, ' \
                          'S.secondary_consultant_id'

FOREIGN_KEY_COLS = 'STRATEGY_ID, CURRENCY_ID, COUNTRY_ID'

FOREIGN_KEY_COLS_WITH_ALIAS = 'S.STRATEGY_ID, S.CURRENCY_ID, S.COUNTRY_ID'

FOREIGN_KEY_COLS_WITH_TABLE_ALIAS = ', country.country_id AS COUNTRY_ID, strategy.strategy_id AS STRATEGY_ID, currency.currency_id AS CURRENCY_ID'

TIMESTAMP_COLUMNS = 'INSERT_UTC_DATETIME, START_DATETIME, END_DATETIME'

HASH_KEY_COLS = 'select hash(SS.AWARD_DATE, ' \
                'SS.CAPACITY_AMOUNT, ' \
                'SS.CAPACITY_NOTES, ' \
                'SS.CAPACITY_REMOVED_DATE, ' \
                'SS.CAPACITY_TRACKING, ' \
                'SS.CAPACITY_TRACKING_CLOSED, ' \
                'SS.CAPACITY_TRACKING_STRATEGY, ' \
                'SS.CONSULTANT_REP_CD, ' \
                'SS.CRM_RELATIONSHIP_ID, ' \
                'SS.ESTIMATED_FEE, ' \
                'SS.ESTIMATED_FUNDING_DATE, ' \
                'SS.ESTIMATED_REVENUE, ' \
                'SS.IS_OPPORTUNITY_CLOSED, ' \
                'SS.NOTES, ' \
                'SS.OPPORTUNITY_CLOSE_DATE, ' \
                'SS.OPPORTUNITY_CURRENT_STATUS, ' \
                'SS.OPPORTUNITY_ENTER_DATE, ' \
                'SS.OPPORTUNITY_RELATIONSHIP_TYPE, ' \
                'SS.OPPORTUNITY_TYPE, ' \
                'SS.ORIGINATION, ' \
                'SS.PORTFOLIO_CODE, ' \
                'SS.PRIMARY_CONSULTANT_CD, ' \
                'SS.PRIMARY_CONTACT, ' \
                'SS.PROBABILITY, ' \
                'SS.REPRESENTATIVE_CD, ' \
                'SS.SALES_PHASE, ' \
                'SS.SECONDARY_CONSULTANT_CD, ' \
                'SS.TARGET_ASSETS, ' \
                'SS.TERRITORY) as source_key'

STAGE_COLS = '(select ' \
             'parse_json($1):awardDate as AWARD_DATE, ' \
             'parse_json($1):capacityAmount as CAPACITY_AMOUNT, ' \
             'parse_json($1):capacityNotes as CAPACITY_NOTES, ' \
             'parse_json($1):capacityRemovedDate as CAPACITY_REMOVED_DATE, ' \
             'parse_json($1):capacityTracking as CAPACITY_TRACKING, ' \
             'parse_json($1):capacityTrackingClosed as CAPACITY_TRACKING_CLOSED, ' \
             'parse_json($1):capacityTrackingStrategy as CAPACITY_TRACKING_STRATEGY, ' \
             'parse_json($1):closeDate as OPPORTUNITY_CLOSE_DATE, ' \
             'parse_json($1):closed as IS_OPPORTUNITY_CLOSED, ' \
             'parse_json($1):consultantRepCode as CONSULTANT_REP_CD, ' \
             'parse_json($1):contactUUID as CONTACT_ID, ' \
             'parse_json($1):countryUUID as COUNTRY_ID, ' \
             'parse_json($1):currencyCodeUUID as CURRENCY_ID, ' \
             'parse_json($1):currentStatus as OPPORTUNITY_CURRENT_STATUS, ' \
             'parse_json($1):enterDate as OPPORTUNITY_ENTER_DATE, ' \
             'parse_json($1):estFundingDate as ESTIMATED_FUNDING_DATE, ' \
             'parse_json($1):estimatedRevenue as ESTIMATED_REVENUE, ' \
             'parse_json($1):feeEstimate as ESTIMATED_FEE, ' \
             'parse_json($1):notes as NOTES, ' \
             'parse_json($1):opportunityId as CRM_OPPORTUNITY_ID, ' \
             'parse_json($1):opportunityType as OPPORTUNITY_TYPE, ' \
             'parse_json($1):origination as ORIGINATION, ' \
             'parse_json($1):portfolioCode as PORTFOLIO_CODE, ' \
             'parse_json($1):primaryConsultantCode as PRIMARY_CONSULTANT_CD, ' \
             'parse_json($1):primaryConsultantCodeUUID as primary_consultant_id, ' \
             'parse_json($1):primaryContact as PRIMARY_CONTACT, ' \
             'parse_json($1):probability as PROBABILITY, ' \
             'parse_json($1):relationshipId as CRM_RELATIONSHIP_ID, ' \
             'parse_json($1):relationshipType as OPPORTUNITY_RELATIONSHIP_TYPE, ' \
             'parse_json($1):relationshipUUID as RELATIONSHIP_ID, ' \
             'parse_json($1):repCode as REPRESENTATIVE_CD, ' \
             'parse_json($1):salesPhase as SALES_PHASE, ' \
             'parse_json($1):secondaryConsultantCode as SECONDARY_CONSULTANT_CD, ' \
             'parse_json($1):secondaryConsultantCodeUUID as secondary_consultant_id, ' \
             'parse_json($1):strategyUUID as STRATEGY_ID, ' \
             'parse_json($1):targetAssets as TARGET_ASSETS, ' \
             'parse_json($1):territory as TERRITORY ' \
             'from @edw_stage_data/crm/opportunity/opportunity.json (file_format => json_format)) SS'

JOIN_CONDITIONS = 'LEFT JOIN RELATIONSHIP AS RELATIONSHIP ON ' \
                  'RELATIONSHIP.crm_relationship_id = trim(SS.RELATIONSHIP_ID) AND ' \
                  'RELATIONSHIP.END_DATETIME = to_date(\'9999-12-31\') ' \
                  'LEFT JOIN CONSULTANT AS CONSULTANT ON ' \
                  'CONSULTANT.crm_consultant_id = trim(SS.primary_consultant_id) AND ' \
                  'CONSULTANT.END_DATETIME = to_date(\'9999-12-31\') ' \
                  'LEFT JOIN CONSULTANT AS CONSULTANT1 ON ' \
                  'CONSULTANT1.crm_consultant_id = trim(SS.secondary_consultant_id) AND ' \
                  'CONSULTANT1.END_DATETIME = to_date(\'9999-12-31\') ' \
                  'LEFT JOIN STRATEGY AS STRATEGY ' \
                  'ON STRATEGY.src_strategy_id = trim(SS.STRATEGY_ID) ' \
                  'AND STRATEGY.END_DATETIME = to_date(\'9999-12-31\') ' \
                  'LEFT JOIN CURRENCY AS CURRENCY ON CURRENCY.crm_currency_id = trim(SS.CURRENCY_ID) ' \
                  'AND CURRENCY.END_DATETIME = to_date(\'9999-12-31\') ' \
                  'LEFT JOIN COUNTRY AS COUNTRY ON COUNTRY.crm_country_id = trim(SS.COUNTRY_ID) ' \
                  'AND COUNTRY.END_DATETIME = to_date(\'9999-12-31\') ' \
                  'LEFT JOIN CONTACT AS CONTACT ON CONTACT.crm_contact_id = trim(SS.CONTACT_ID) ' \
                  'AND CONTACT.END_DATETIME = to_date(\'9999-12-31\')) AS SRC'


# Fixtures
@pytest.fixture
def dynamic_statement_generator():
    return DynamicStatementGenerator('@edw_stage_data/crm/opportunity/opportunity.json',
                                     'opportunity', True)


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
        'INSERT into {} ({}, {}, {})'.format(TABLE_NAME, IDENTIFIER_COL,
                                             COLUMN_NAMES,
                                             TIMESTAMP_COLUMNS),
        answer)


def test_generate_select_statement_with_column_alias_and_datetime_columns(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_statement_with_attributes()
    verify_answer('SELECT S.source_key, {}, '
                  'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), '
                  'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), '
                  'to_date(\'9999-12-31\') from'
                  .format(COLUMN_NAMES_WITH_ALIAS), answer)


def test_generate_select_statement_without_column_alias_and_datetime_columns(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_statement_with_attributes(False, False)
    verify_answer('SELECT source_key, {} from'.format(COLUMN_NAMES), answer)


def test_generate_select_with_hash_table_and_foreign_table_cols(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_with_hashkey_and_foreign_table_cols()
    verify_answer('{}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_FOREIGN_COLS), answer)


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
    select_with_cols_and_foreign_keys = 'SELECT source_key, {} from'.format(COLUMN_NAMES)
    select_with_hash_cols_and_foreign_key_cols = '{}, {} from' \
        .format(HASH_KEY_COLS, COLUMN_NAMES_WITH_FOREIGN_COLS)
    select_with_stage_data = '{}'.format(STAGE_COLS)
    join_conditions = '{}){newline_and_tab}AS S'.format(JOIN_CONDITIONS, newline_and_tab=NEWLINE_AND_TAB)
    verify_answer(answer,
                  '{0} from{newline_and_tab}{tab}({1}{tab}{newline_and_tab}{tab}({2}{newline_and_tab}{tab}{3}{newline_and_tab}{tab}{4}'.format(
                      select_with_column_alias_and_current_time,
                      select_with_cols_and_foreign_keys,
                      select_with_hash_cols_and_foreign_key_cols,
                      select_with_stage_data,
                      join_conditions,
                      newline_and_tab=NEWLINE_AND_TAB,
                      newline=NEWLINE,
                      tab=TAB))


def test_generate_join_conditions(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_source_key_outer_join_conditions()
    verify_answer('LEFT JOIN OPPORTUNITY T ON T.OPPORTUNITY_ID = '
                  'S.source_key WHERE T.OPPORTUNITY_ID is null', answer)


def test_generate_select_from_stage(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_select_from_stage()
    allcols_including_foreign_and_hash = '{}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_FOREIGN_COLS)
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
    insert_cols = 'INSERT into {} (OPPORTUNITY_ID, {}, {})'.format(TABLE_NAME, COLUMN_NAMES, TIMESTAMP_COLUMNS)
    select_with_column_alias_and_current_time = 'SELECT S.source_key, {}, ' \
                                                'to_timestamp_ntz(convert_timezone(\'UTC\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))), ' \
                                                'to_date(\'9999-12-31\')' \
        .format(COLUMN_NAMES_WITH_ALIAS)
    select_with_cols_and_foreign_keys = 'SELECT source_key, {} from\t'.format(COLUMN_NAMES)
    select_with_hash_cols_and_foreign_key_cols = '({}, {} from'.format(HASH_KEY_COLS, COLUMN_NAMES_WITH_FOREIGN_COLS)
    select_with_stage_data = '{}'.format(STAGE_COLS)
    join_conditions = JOIN_CONDITIONS
    primary_key_null_condition = 'AS S{newline}LEFT JOIN OPPORTUNITY T ' \
                                 'ON T.OPPORTUNITY_ID = S.source_key WHERE T.OPPORTUNITY_ID is null'. \
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
    return 'UPDATE OPPORTUNITY set END_DATETIME = ' \
           'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4))) ' \
           'where OPPORTUNITY_ID ' \
           'not in {newline_and_tab}({0}{newline}{tab}' \
           'from{newline_and_tab}{tab}{1}{newline_and_tab})and END_DATETIME = to_date(\'9999-12-31\')'.format(
        HASH_KEY_COLS, STAGE_COLS,
        newline_and_tab=NEWLINE_AND_TAB, newline=NEWLINE, tab=TAB)


def test_generate_insert_and_update_statements(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_insert_and_update_statements()
    verify_answer(insert_statement_generator(), answer.get("INSERT_STATEMENT"))
    verify_answer(update_statement_generator(), answer.get("UPDATE_STATEMENT"))


def test_primary_key_check_query(dynamic_statement_generator):
    answer = dynamic_statement_generator.generate_primary_key_check_query()
    expected_result = 'select * from (' \
                      'select  trim(parse_json($1):opportunityId) as opportunityId, ' \
                      'count(parse_json($1):opportunityId) as count ' \
                      'from @edw_stage_data/crm/opportunity/opportunity.json (file_format => json_format) ' \
                      'group by opportunityId' \
                      ') as SS where SS.count > 1'

    verify_answer(expected_result, answer)
