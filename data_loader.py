import base64
import json
import logging
import os
from datetime import datetime
from enum import Enum

import boto3
import requests
import snowflake.connector
from botocore.exceptions import ClientError
from requests.auth import _basic_auth_str

from source.dynamic_sql_generator import DynamicStatementGenerator

STATUS_TABLE = 'edw-status'
DYNAMO_DB_RESOURCE_NAME = 'dynamodb'
logging.root.handlers = []
logger = logging.getLogger('root')
FORMAT = "[%(levelname)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)


class FeedStatus(Enum):
    SUCCESS = "Success"
    FAIL = "Fail"
    IN_PROGRESS = "In Progress"
    RE_PROCESSING = "Re-processing"


def is_token_valid(auth_token):
    auth_server_url = os.environ['AUTHORIZATION_SERVER_URL']
    validate_auth_token_url = auth_server_url + '/v1/introspect'
    logger.info('validate_auth_token_url : ' + validate_auth_token_url)

    # Get client id and secret from secret manager
    client_credentials = get_secret(os.environ['LOADER_SERVICE_SECRET'], os.environ['REGION'])

    if (client_credentials != None):
        client_credentials_dict = json.loads(client_credentials)
        client_id = client_credentials_dict['client_id']
        client_secret = client_credentials_dict['client_secret']

        auth_token = (auth_token).replace('Bearer', '').lstrip()
        validate_params = {'token_type_hint': 'access_token', 'token': auth_token}

        if not client_id:
            logger.error('client_id  has no value')
            return False
        elif not client_secret:
            logger.error('client_secret has no value')
            return False
        else:
            validate_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
                'Authorization': _basic_auth_str(client_id, client_secret)
            }
            try:
                validate_response = requests.post(validate_auth_token_url,
                                                  headers=validate_headers,
                                                  data=validate_params)
                logger.info(validate_response.content)

                if (validate_response != None):
                    validation_response_dict = json.loads(validate_response.text)
                    is_token_valid = validation_response_dict['active']
                    logger.info('Token passed is valid:' + str(is_token_valid))
                    return is_token_valid
                else:
                    logger.error('validate response content is None')
                    return False
            except requests.exceptions.HTTPError as _http_ex:
                logger.error('Http error has occured : ' + _http_ex)
                return False
            except requests.exceptions.Timeout as _timeout_ex:
                logger.error('Timeout error has occured : ' + _timeout_ex)
                return False
            except requests.exceptions.RequestException as _request_ex:
                logger.error('Request exception has occured : ' + _request_ex)
                return False
    else:
        logger.error('client credentials from secret manager is None')
        return False


def get_secret(sec_name, region):
    secret_name = sec_name
    region_name = region

    sec_val = None
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            sec_val = get_secret_value_response['SecretString']
        else:
            sec_val = base64.b64decode(get_secret_value_response['SecretBinary'])

    return sec_val


def execute_snowsql(conn_obj, insert_and_merge_statements):
    # Instantiate snowflake connector
    ctx = initialize_snowflake_context(conn_obj)
    ctx.autocommit(False)
    cursor_list = ctx.cursor()
    try:
        insert_sql = insert_and_merge_statements.get('INSERT_STATEMENT')
        update_sql = insert_and_merge_statements.get('UPDATE_STATEMENT')
        logger.info('Insert Statement: {}'.format(insert_sql))
        cursor1 = cursor_list.execute(insert_sql)
        logger.info('Insert Statement: executed {} '.format(cursor_list))
        for cursor in cursor1:
            for row in cursor:
                logger.info(f'Rows Affected by insert: {row}')
                logger.info(f'Update Statement: {update_sql} ')
                cursor2 = cursor_list.execute(update_sql)
                logger.info(f'Update Statement: executed {cursor_list}')
                for c in cursor2:
                    for r in c:
                        logger.info(f'Rows Affected by update: {r}')

        logger.info('Committing')
        ctx.commit()
    except Exception as ex:
        logger.error(f'Failed to execute query {ex}', exc_info=True)
        ctx.rollback()
        raise
    finally:
        ctx.close()


def execute_select(conn_obj, select_statement):
    ctx = initialize_snowflake_context(conn_obj)
    ctx.autocommit(False)
    cursor_list = ctx.cursor()
    try:
        logger.info('Select Statement: {}'.format(select_statement))
        cursor = cursor_list.execute(select_statement)
        logger.info('Select Statement: executed {} '.format(cursor_list))
        for cursor in cursor:
            for row in cursor:
                logger.info(' Rows returned by select: {}'.format(row))
                return row
    except Exception as ex:
        logger.error(f'Failed to execute query {ex}', exc_info=True)
        raise
    finally:
        ctx.close()


def initialize_snowflake_context(conn_obj):
    logger.info('Warehouse: {} Database: {} Schema: {}'.format(conn_obj['warehouse'], conn_obj['database'],
                                                               conn_obj['schema']))
    return snowflake.connector.connect(
        user=conn_obj['user'],
        password=conn_obj['password'],
        account=conn_obj['account'],
        warehouse=conn_obj['warehouse'],
        database=conn_obj['database'],
        schema=conn_obj['schema']
    )


def response_status(code, body):
    response = {
        "statusCode": code,
        "statusDescription": "",
        "isBase64Encoded": False,
        "headers": {
            "Content-Type": "text/html; charset=utf-8"
        }
    }

    response['body'] = """<html>
    <head>
    <title>EDW Data Loader</title>
    <style>
    html, body {
    margin: 0; padding: 0;
    font-family: arial; font-weight: 700; font-size: 3em;
    text-align: center;
    }
    </style>
    </head>
    <body>
    <p>""" + body + """</p>
    </body>
    </html>"""
    return response


def send_notification(domain, msg):
    topic_name = None
    topic_arn = None

    # Create SNS client
    sns = boto3.client('sns')

    # Get a list of all topic ARNs from the response
    response = sns.list_topics()

    # Find specific topic
    for topic in response['Topics']:
        if domain == topic['TopicArn'].split(':')[5]:
            topic_name = topic['TopicArn'].split(':')[5]
            topic_arn = topic['TopicArn']

    if topic_name is None:
        # Create topic and send publish to SNS
        created_topic = sns.create_topic(Name=domain)
        logger.info(f"Topic {created_topic['TopicArn']} has been created")
        # Publish a message to the specified SNS topic
        topic_response = sns.publish(
            TopicArn=created_topic['TopicArn'],
            Message=msg
        )
        logger.info(f"Published to {created_topic} topic")
        logger.info(topic_response)
    else:
        # Publish a message to the specified SNS topic
        topic_response = sns.publish(
            TopicArn=topic_arn,
            Message=msg
        )

        logger.info(f"Topic exists: {topic_name}")
        logger.info(f"Publishing to {topic_arn} topic")

        # Print out the response
        logger.info(topic_response)

    return topic_response


def get_object(bucket_name, object_name):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
        logger.info(response)
    except ClientError as e:
        # AllAccessDisabled error == bucket or object not found
        logger.error(e)
        raise e
    string_object = json.loads(response['Body'].read())
    return string_object


def insert_status_record(object_key, feed_name, feed_status):
    logger.info(f'Insert status record {object_key} with status {feed_status}')
    table = get_status_table()
    insert_date = get_update_date()

    response = table.put_item(
        Item={
            'objectKey': object_key,
            'feedStatus': feed_status.value,
            'insertDate': insert_date,
            'feedName': feed_name,
            'updateDate': None
        }
    )


def get_status_table():
    dynamo_db = boto3.resource(DYNAMO_DB_RESOURCE_NAME, region_name='us-east-1')
    table = dynamo_db.Table(STATUS_TABLE)
    return table


def update_status_record(object_key, feed_status):
    logger.info(f'Update status record {object_key} with status {feed_status}')
    table = get_status_table()
    update_date = get_update_date()
    response = table.update_item(
        Key={
            'objectKey': object_key
        },
        UpdateExpression="set feedStatus = :s, updateDate=:d",
        ExpressionAttributeValues={
            ':s': feed_status.value,
            ':d': update_date
        },
        ReturnValues="UPDATED_NEW"
    )

    logger.info("UpdateItem succeeded:")
    logger.info(json.dumps(response, indent=4))


def get_update_date():
    now = datetime.now()
    update_date = now.strftime("%d/%m/%Y %H:%M:%S")
    return update_date


def item_exist(object_key):
    table = get_status_table()

    try:
        response = table.get_item(
            Key={
                'objectKey': object_key
            }
        )
        logger.info(response)
        if 'Item' in response.keys():
            return True
        else:
            return False
    except ClientError as e:
        logger.error(e.response['Error']['Message'])


def get_feed_status(object_key):
    table = get_status_table()

    try:
        response = table.get_item(
            Key={
                'objectKey': object_key
            }
        )
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
    else:
        item = response['Item']
        logger.info(response['Item']['feedStatus'])
        logger.info("GetItem succeeded:")
        logger.info(json.dumps(item, indent=4))
    return response['Item']['feedStatus']


def check_file_exists(p_bucket, p_key):
    logger.info(f'Checking if S3 file exists or not for bucket:{p_bucket} and key:{p_key}')
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(p_bucket)
        # bucket.Object(p_key).last_modified
        file_size = bucket.Object(p_key).content_length
        if file_size > 0:
            logger.info(f'Success : {p_key} key found and its size {file_size} bytes')
            return True
        else:
            logger.info(f'Failed : {p_key} key found but its size is {file_size} bytes!')
            return False

    except ClientError as ex:
        if ex.response['Error']['Message'] == 'Not Found':
            logger.info(f'Failed : No Such key found-{p_key}')
        else:
            logger.info(f'Failed : Error found while checking for file exists')
        return False
    except:
        logger.info(f'Failed : Unknown Error while checking for file exists')
        return False


def handler(event, context):
    response = None
    metadata_dict = None
    feed_domain = 'edw-generic'
    object_name = ''
    feed_status = ''
    is_runnable = True
    try:
        # Get auth param
        # logger.info((event))
        # authorization_token = event["headers"]['authorization_token']
        # logger.info("Auth token: " + authorization_token)

        # Validate the auth token before proceeding in any action....
        # if(is_token_valid(authorization_token)): #this should be True for valid condition.
        #    logger.info("True value returned for token status")
        # else:
        #    logger.info("False value is returned for token status")

        # getting query params for the processing

        stage_name = event["queryStringParameters"]['stageName']
        feed_name = event["queryStringParameters"]['feedName']
        bucket_name = event["queryStringParameters"]['bucketName']
        object_name = event["queryStringParameters"]['objectName']

        # sample Object key= @edw_stage_data/crm/consultant/2019/10/22/consultant-2019-10-22-21:00:26.json
        # Below code excludes stage and get only key
        key_name = object_name[object_name.find('/') + 1:]
        logger.info(f'Extracted key_name : {key_name}')

        logger.info(
            f'Stage name: {stage_name}, Feed name: {feed_name}, Bucket name {bucket_name}, ObjectKey {object_name}')

        insert_status_record(stage_name, feed_name, FeedStatus.IN_PROGRESS)

        if not check_file_exists(bucket_name, key_name):
            raise Exception(
                'Failed to process feed ' + feed_name + ' and ' + object_name + '. S3 file - '
                + bucket_name + '/' + key_name + ' does not exist. Please check log/reference table and resolve issues')

        logger.info(f'Feed is runnable = {is_runnable}')

        dynamic_statement_generator = DynamicStatementGenerator(stage_name, feed_name)
        logger.info('$ DynamicStatementGenerator {}'.format(dynamic_statement_generator))

        # getting metadata
        metadata_dict = dynamic_statement_generator.metadata_dict

        logger.info(json.dumps(metadata_dict))

        feed_domain = metadata_dict['feedDomain']
        logger.info(feed_domain)
        logger.info(metadata_dict['feedName'])

        # check if domain feed exists
        if feed_name.upper() != metadata_dict['feedName'].upper():
            raise Exception('Domain ' + feed_name + ' is not registered or missing from the metadata')

        ref_integrity_check_sql = dynamic_statement_generator.referential_check_sql()
        sec_val = get_secret(os.environ['SECRETS_MANAGER'], os.environ['REGION'])
        sec_dict = json.loads(sec_val)

        result = execute_select(sec_dict, ref_integrity_check_sql)
        logger.info(f'Reference check result: {result}')

        if result is not None and result.find('Missing') != -1:
            logger.warning(f' Feed {feed_name} reference check status- Fail. Failure is [{result}]')
            raise Exception(
                f'Failed to process feed ' + feed_name + ' and ' + object_name + '. Following reference checks failed [' + result + ']. '
                                                                                                                                    f'Please resolve issues and re-run feed')
        else:
            logger.info(f' Feed {feed_name} reference check status- Success')
            insert_and_merge_statements = dynamic_statement_generator.generate_insert_and_update_statements()
            execute_snowsql(sec_dict, insert_and_merge_statements)

        response = response_status(200, f'Feed {feed_name} loaded successfully')
        update_status_record(stage_name, FeedStatus.SUCCESS)
    except KeyError as key_error:
        if object_name != '' and feed_status != FeedStatus.FAIL:
            update_status_record(stage_name, FeedStatus.FAIL)
        logger.error(f'Query string error {str(key_error)} in feed {feed_name}', exc_info=True)
        send_notification(feed_domain, f'Parameter value {str(key_error)} is not provided')
        response = response_status(400, f'Parameter value {str(key_error)} is not provided')
    except Exception as ex:
        if object_name != '' and is_runnable == True:
            update_status_record(stage_name, FeedStatus.FAIL)
        logger.error(
            f'Failed to load feed {feed_name} using metadata arguments {metadata_dict}. Failure Reason: {str(ex)}',
            exc_info=True)
        response = response_status(400, f'Failed to load feed {feed_name}. Failure Reason: [{str(ex)}]')
        send_notification(feed_domain, str(ex))

    return response
