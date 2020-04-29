"""
dynamic_sql_generator.py contains the DynamicStatementGenerator class for creating
a snowflake query to move data from stage to snowflake.
"""
import collections as collection
import json
import logging
import os
from builtins import property

from botocore.vendored import requests

logging.root.handlers = []
logger = logging.getLogger('root')
FORMAT = "[%(levelname)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)


class DynamicStatementGenerator(object):
    NEWLINE = '\n'
    TAB = '\t'
    NEWLINE_AND_TAB = '\n\t'
    METADATA_URL = 'METADATA_URL'
    TIMESTAMP_COLUMNS = collection.OrderedDict(
        [("INSERT_UTC_DATETIME", "to_timestamp_ntz(convert_timezone('UTC', CURRENT_TIMESTAMP(4)))"),
         ("START_DATETIME", "to_timestamp_ntz(convert_timezone('America/New_York', CURRENT_TIMESTAMP(4)))"),
         ("END_DATETIME", "to_date('9999-12-31')")])
    SNOWFLAKE_CURRENT_TIME = 'to_timestamp_ntz(convert_timezone(\'America/New_York\', CURRENT_TIMESTAMP(4)))'
    HASH_COLS_NAME = 'source_key'
    END_DATETIME = 'END_DATETIME'
    END_DATETIME_VALUE = '9999-12-31'
    META_DATA_URL = ''
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # logger.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s')

    def __init__(self, stage_name, feed_name, test=False):
        self.TEST = test
        self.log('Initializing dynamic SQL generator with stage name : {} feed name : {} test environment : {}'.format(
            stage_name, feed_name, test))
        self.STAGE_NAME = stage_name

        if not self.TEST:
            self.META_DATA_URL = os.environ[('%s' % self.METADATA_URL)]
        meta_data = self.read_meta_data(feed_name)
        self.META_DATA_DICT = json.loads(meta_data)
        self.KEYS = self.META_DATA_DICT.keys()
        self.BUCKET_NAME = self.META_DATA_DICT.get('S3BucketName', '')
        self.FEED_NAME = self.META_DATA_DICT.get('feedName', '')
        self.FEED_ALIAS = 'S'
        self.TEMP_VIEW = 'SS'
        self.TABLE_ALIAS = 'T'
        self.DESTINATION_TABLE_NAME = self.META_DATA_DICT.get('targetRDSTable', '')
        self.JOIN_COLUMNS = self.META_DATA_DICT.get('RIConstraint', '')
        self.IDENTIFIER_COLUMN = self.META_DATA_DICT.get('identifierAttributes', '')
        self.S3_FEED_FILE_LOCATION = stage_name

    @property
    def metadata_dict(self):
        return self.META_DATA_DICT

    @property
    def metadata_url(self):
        return self.META_DATA_URL

    def get_timestamp_column_names(self):
        timestamp_columns = []
        for (key, val) in self.TIMESTAMP_COLUMNS.items():
            timestamp_columns.append(key)
        return timestamp_columns

    '''
    Function that reads meta_data corresponding to the feed name from dynamoDB. In developers local environment if
    the last argument Test is passed as True then it reads the meta data from data folder under tests. By default this 
    argument is set to False
    '''

    def read_meta_data(self, feed_name):
        json_str = ''
        if self.TEST:
            self.log('Reading from metadata file')
            meta_data_file = './data/{}-metadata.json'.format(feed_name)
            self.log('Opening metadata file {} in test mode '.format(meta_data_file))
            try:
                with open(meta_data_file) as json_file:
                    result = json.load(json_file)
                    json_str = json.dumps(result)
            except Exception as ex:
                self.log('Unable to open metadata file {} due to {} '.format(meta_data_file, ex), ex)
                raise

        else:
            self.log('Reading from dynamoDB through RestAPI')
            try:
                post_data = json.dumps({
                    "action": "get",
                    "feedName": "{}".format(feed_name)
                })
                self.log('Post URL: {} Data: {}'.format(self.metadata_url, post_data))
                response = requests.post(self.metadata_url, headers={'content-type': 'application/json'},
                                         data=post_data)
                response_json = json.loads(response.text)
                json_str = json.dumps(response_json)
                self.log('Response is a = {}'.format(json_str))
                self.log('Response type is a = {}'.format(type(json_str)))
            except Exception as ex:
                self.log('Unable to read meta data {} from dynamoDB due to {}'.format(feed_name, ex), ex)
                raise
        return json_str

    ('\n'
     '    Function that gets column names corresponding to the feed from the metadata and adds the timestamp columns'
     '    before returning a list of column names\n'
     '')

    def get_column_names(self, include_datetime_cols=True):
        col_names = self.get_column_names_without_audit_cols()
        if include_datetime_cols:
            col_names += self.get_timestamp_column_names()
        self.log('All columns {} '.format(col_names))
        return col_names

    def get_column_names_without_audit_cols(self):
        columns_from_metadata = self.metadata_dict['columnList']
        col_names = []  # This is required and do not inline it.
        col_names += self.metadata_dict.get('identifierAttributes')
        columns_with_no_table_column = list(filter(lambda col_name:
                                                   True if ((col_name.get('tableColumn') is not None)) else False,
                                                   columns_from_metadata))
        col_names_without_identifier = list(map(lambda col_name: col_name.get('tableColumn'), columns_from_metadata))
        col_names_without_identifier.sort()
        col_names += col_names_without_identifier
        return col_names

    def gen_column_names_including_foreign_table_columns(self):
        columns_from_metadata = self.metadata_dict['columnList']
        col_names = []  # This is required and do not inline it.
        columns_with_no_special_handling = list(filter(lambda col_name:
                                                       True if ((col_name.get('tableColumn') is not None)
                                                                and (col_name.get(
                                                                   'useAsForeignKey') != "true")) else False,
                                                       columns_from_metadata))
        col_names += list(
            map(lambda col_name: 'SS.{}'.format(col_name.get('tableColumn')), columns_with_no_special_handling))
        columns_with_special_handling = list(
            filter(lambda col_name: True if ((col_name.get('useAsForeignKey') == "true")) else False,
                   columns_from_metadata))
        col_names += list(map(lambda col_name: '{}.{} AS {}'.format(
            DynamicStatementGenerator.clean_table_name(col_name.get('foreignKeyTable')),
            col_name.get('foreignKeySelectColumn'),
            col_name.get('tableColumn')), columns_with_special_handling))
        col_names.sort()
        return col_names

    def generate_insert_into_statement_column_names_clause(self):
        main_table_columns = ', '.join(self.get_column_names())
        return 'INSERT into {0} ({1})'.format(self.DESTINATION_TABLE_NAME, main_table_columns)

    def generate_insert_into_statement_select_clause(self):
        first_select_with_cols_and_datetime_and_alias = self.generate_select_statement_with_attributes()
        second_select_with_cols_only = self.generate_select_statement_with_attributes(False, False)
        select_with_stage_data = self.generate_select_from_stage()
        return '{0}{newline_and_tab}{tab}({1}{tab}{2}){newline}{tab}AS S'.format(
            first_select_with_cols_and_datetime_and_alias,
            second_select_with_cols_only,
            select_with_stage_data,
            newline_and_tab=self.NEWLINE_AND_TAB,
            newline=self.NEWLINE,
            tab=self.TAB)

    def generate_insert_statement_with_all_conditions(self):
        insert_clause_of_final_statement = self.generate_insert_into_statement_column_names_clause()
        select_for_insert_values = self.generate_insert_into_statement_select_clause()
        join_conditions = self.generate_source_key_outer_join_conditions()
        return '{0}{newline_and_tab}{1}{newline}{2}'.format(
            insert_clause_of_final_statement,
            select_for_insert_values,
            join_conditions,
            newline_and_tab=self.NEWLINE_AND_TAB,
            newline=self.NEWLINE,
            tab=self.TAB)

    def generate_select_statement_with_attributes(self, alias_required=True, datetime_cols_required=True):
        sql = ''
        column_names = []
        if datetime_cols_required:
            column_names = self.get_column_names()
        else:
            column_names = self.get_column_names(False)
        column_list = self.metadata_dict.get('columnList')
        add_comma = False
        for col in column_names:
            sql += ', ' if add_comma else ''
            if col in self.get_timestamp_column_names():
                sql += '{}'.format(self.TIMESTAMP_COLUMNS[col])
            # Replace the hash column name value with source_key
            elif col in self.metadata_dict.get('identifierAttributes'):
                if alias_required:
                    sql += '{}.{}'.format(self.FEED_ALIAS, self.HASH_COLS_NAME)
                else:
                    sql += '{}'.format(self.HASH_COLS_NAME)
            else:
                column_with_sql = list(filter(lambda column_dict: column_dict.get('tableColumn') == col, column_list))
                if column_with_sql is not None and len(column_with_sql) > 0 and column_with_sql[0].get(
                        'value') is not None:
                    inner_select_as_value = '\'{}\''.format(column_with_sql[0].get('value'))
                    self.log('{} '.format(inner_select_as_value))
                    sql += inner_select_as_value
                else:
                    if alias_required:
                        sql += '{}.{}'.format(self.FEED_ALIAS, col)
                    else:
                        sql += '{}'.format(col)
            add_comma = True
        return 'SELECT {} from'.format(sql)

    def generate_select_with_hashkey_and_foreign_table_cols(self):
        source_key_using_hash_cols = self.generate_source_key_using_hash_cols()
        col_names_including_foreign_cols = self.gen_column_names_including_foreign_table_columns()
        return 'select {}, {} from'.format(source_key_using_hash_cols,
                                           ', '.join(col_names_including_foreign_cols))

    def generate_source_key_using_hash_cols(self):
        hash_col_names = []
        for col_data in self.metadata_dict.get('columnList'):
            if col_data.get('captureHistory') == 'true':
                col_name = col_data.get('tableColumn')
                col_name = '{}.{}'.format('SS', col_name)

                hash_col_names.append(col_name)
        hash_col_names.sort()
        return 'hash({}) as {}'.format(', '.join(hash_col_names), self.HASH_COLS_NAME)

    def generate_select_using_stage_columns(self):
        feed_col_table_col_mapping = {}
        for dict_entry in self.metadata_dict.get('columnList'):
            feed_col_table_col_mapping[
                dict_entry.get('feedAttribute', 'MISSING_METADATA_FEED_ATTRIBUTE')] = dict_entry.get('tableColumn',
                                                                                                     'MISSING_METADATA_TABLE_COL')
        sql = 'select '
        add_comma = False
        feed_col_table_col_mapping_sorted = dict(sorted(feed_col_table_col_mapping.items()))
        for feedAttributeName, tableColumnName in feed_col_table_col_mapping_sorted.items():
            sql += ', ' if add_comma else ''
            if tableColumnName == 'MISSING_METADATA_TABLE_COL':
                sql += 'parse_json($1):{} as {}'.format(feedAttributeName, feedAttributeName)
            else:
                sql += 'parse_json($1):{} as {}'.format(feedAttributeName, tableColumnName)
            add_comma = True
        sql += ' from {} (file_format => json_format)'.format(self.S3_FEED_FILE_LOCATION)
        return '({}) SS'.format(sql)

    def generate_source_key_outer_join_conditions(self):
        join_conditions = 'LEFT JOIN {} {} '.format(self.DESTINATION_TABLE_NAME, self.TABLE_ALIAS)
        add_comma = False
        if len(self.metadata_dict.get('identifierAttributes')) > 0:
            join_conditions += 'ON '
            for identifier_attribute in self.metadata_dict.get('identifierAttributes'):
                join_conditions += ', ' if add_comma else ''
                join_conditions += '{}.{} = {}.{} WHERE {}.{} is null'.format(self.TABLE_ALIAS, identifier_attribute,
                                                                              self.FEED_ALIAS, self.HASH_COLS_NAME,
                                                                              self.TABLE_ALIAS, identifier_attribute)
                add_comma = True

        return join_conditions

    def generate_select_from_stage(self):
        select_with_hash_and_foreign_key_cols = self.generate_select_with_hashkey_and_foreign_table_cols()
        select_with_stage_data = self.generate_select_using_stage_columns()
        foreign_key_join_clause = self.join_to_foreign_tables()
        return '{newline_and_tab}{tab}({0}{newline_and_tab}{tab}{1}{newline_and_tab}{tab}{2}) AS SRC'.format(
            select_with_hash_and_foreign_key_cols,
            select_with_stage_data,
            foreign_key_join_clause,
            newline_and_tab=self.NEWLINE_AND_TAB,
            tab=self.TAB)

    def join_to_foreign_tables(self):
        join_conditions = ''
        columns_from_metadata = self.metadata_dict['columnList']
        join_data = []  # This is required and do not inline it.
        columns_for_join_conditions = list(filter(lambda col_name:
                                                  True if ((col_name.get('foreignKeyTable') is not None)) else False,
                                                  columns_from_metadata))
        col_aliases = []
        for col_name in columns_for_join_conditions:
            col_alias = DynamicStatementGenerator.clean_table_name(col_name.get('foreignKeyTable'))
            unique_int = 1
            if col_alias in col_aliases:
                col_alias += str(unique_int)
                unique_int += 1
            col_aliases.append(col_alias)

            join_conditions += 'LEFT JOIN {foreign_key_table} AS {foreign_key_table_without_schema} ' \
                               'ON {foreign_key_table_without_schema}.{foreign_key_column} = trim({temp_view_alias}.{table_column}) ' \
                               'AND {foreign_key_table_without_schema}.{end_date} = {future_date} ' \
                .format(
                foreign_key_table=col_name.get('foreignKeyTable'),
                foreign_key_table_without_schema=col_alias,
                foreign_key_column=col_name.get('foreignKeyColumn'),
                temp_view_alias=self.TEMP_VIEW,
                table_column=col_name.get('tableColumn'),
                end_date=self.END_DATETIME,
                future_date=self.TIMESTAMP_COLUMNS.get(self.END_DATETIME, ''))
        return join_conditions.strip()

    def generate_update_statement(self):
        source_key_with_hash_columns = 'select {}'.format(self.generate_source_key_using_hash_cols())
        stage_cols = self.generate_select_using_stage_columns()
        return 'UPDATE {0} set {1} = {current_time} where {2} not in {newline_and_tab}({3}{newline}{tab}' \
               'from{newline_and_tab}{tab}{4}{newline_and_tab})and END_DATETIME = {5}'.format(
            self.DESTINATION_TABLE_NAME,
            self.END_DATETIME,
            self.IDENTIFIER_COLUMN[0],
            source_key_with_hash_columns,
            stage_cols,
            self.TIMESTAMP_COLUMNS.get(self.END_DATETIME),
            current_time=self.SNOWFLAKE_CURRENT_TIME,
            newline_and_tab=self.NEWLINE_AND_TAB,
            newline=self.NEWLINE,
            tab=self.TAB)

    def referential_check_sql(self):
        columns_from_metadata = self.metadata_dict['columnList']
        col_names = []
        columns_with_special_handling = list(
            filter(lambda col_name: True if ((col_name.get('useAsForeignKey') == "true")) else False,
                   columns_from_metadata))
        col_names += list(map(lambda col_name: '{}.{} '.format(
            DynamicStatementGenerator.clean_table_name(col_name.get('foreignKeyTable')),
            col_name.get('foreignKeySelectColumn')), columns_with_special_handling))
        #
        if len(col_names) > 0:
            sql = 'select max( distinct {}'.format(' '.join(
                ["case when " + col + " is null then \'Missing " + col + "data \' else \'\' end ||" for col in
                 col_names]))
            sql += ' \'\' ) as result from'
            print(sql)
            sql += '{} {} '.format(
                self.generate_select_using_stage_columns(),
                self.join_to_foreign_tables())
        else:
            sql = 'select \'\''

        self.log(sql)
        return sql

    def generate_insert_and_update_statements(self):
        insert_statement = self.generate_insert_statement_with_all_conditions()
        update_statement = self.generate_update_statement()
        return collection.OrderedDict([("INSERT_STATEMENT", insert_statement), ("UPDATE_STATEMENT", update_statement)])

    def generate_primary_key_check_query(self):
        column_name_conditions = ''
        group_by_conditions = ''
        add_comma = False
        columns_from_metadata = self.metadata_dict['columnList']
        primary_key_columns = list(map(lambda col_name: col_name.get('feedAttribute'), filter(lambda col_name:
                                                                                              True if ((col_name.get(
                                                                                                  'tableColumn') is not None)
                                                                                                       and (
                                                                                                               col_name.get(
                                                                                                                   'primaryKey') == "true")) else False,
                                                                                              columns_from_metadata)))
        for col_name in primary_key_columns:
            column_name_conditions += ', ' if add_comma else ''
            column_name_conditions += f'trim(parse_json($1):{col_name}) as {col_name}'
            group_by_conditions += ', ' if add_comma else ''
            group_by_conditions += f'{col_name}'
            add_comma = True
        self.log(column_name_conditions)
        primary_key_check_query = f'select * from (select {column_name_conditions}' \
                                  f', count(*) as count ' \
                                  f'from {self.S3_FEED_FILE_LOCATION} (file_format => json_format) ' \
                                  f'group by {group_by_conditions}) as SS where SS.count > 1'
        return primary_key_check_query

    @staticmethod
    def clean_table_name(table_name_with_qualifiers):
        if table_name_with_qualifiers.find('.') != -1:
            index_of_last_period = table_name_with_qualifiers.rindex('.') + 1
            return table_name_with_qualifiers[index_of_last_period:]
        else:
            return table_name_with_qualifiers

    def log(self, log_statement, ex=None):
        if self.TEST:
            print(log_statement)
        elif ex == None:
            logger.info(log_statement)
        else:
            logger.error(log_statement, ex)
