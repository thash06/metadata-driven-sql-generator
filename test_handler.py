import os

import boto3
import moto
import pytest

import data_loader

EVENT_FILE = os.path.join(
    os.path.dirname(__file__),
    'tests',
    'data',
    'event.json'
)
SOURCE_BUCKET = "source_bucket"


@pytest.fixture()
def event(event_file=EVENT_FILE):
    return {
        "queryStringParameters":
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2016-09-25T05:15:44.261Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AROAW5C"
                },
                "stageName": "aStageName",
                "feedName": "aFeedName",
                "bucketName": "aBucketName",
                "objectName": "anObjectName",
                "responseElements": {
                    "x-amz-request-id": "00093EEAA5C7G7F2",
                    "x-amz-id-2": "9tTklyI/OEj"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "151dfa64",
                    "bucket": {
                        "name": SOURCE_BUCKET,
                        "ownerIdentity": {
                            "principalId": "A3QLJ3P3P5QY05"
                        },
                        "arn": "arn:aws:s3:::" + SOURCE_BUCKET
                    },
                    "object": {
                        "key": "SomeKey",
                        "size": 11,
                        "eTag": "5eb63bbb",
                        "sequencer": "0057E75D80IA35C3E0"
                    }
                }
            }

    }


@pytest.fixture()
def cfn_stack_name():
    '''Return name of stack to get Lambda from'''
    # FIXME: We should eventually read serverless.yml to figure it out.
    # Handling different environments would be good too.
    return 'data_loader'


@pytest.fixture()
def lambda_client():
    '''Lambda client'''
    return boto3.client('lambda', region_name='us-east-2',
                        aws_access_key_id='ACCESS_ID',
                        aws_secret_access_key='ACCESS_KEY')


@pytest.fixture()
def boto3_resource():
    return boto3.resource(
        "dynamodb",
        region_name="eu-west-1",
        aws_access_key_id="fake_access_key",
        aws_secret_access_key="fake_secret_key",
    )


@pytest.fixture()
def lambda_function(cfn_stack_name):
    '''Return Lambda function name'''
    return '-'.join([cfn_stack_name, 'handler'])


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['REGION'] = 'region'


TEST_BUCKET_NAME = 'test-bucket'
TEST_DYNAMO_TABLE_NAME = 'test-dynamodb-table'


@pytest.fixture
def s3_bucket():
    with moto.mock_s3():
        boto3.client('s3').create_bucket(Bucket=TEST_BUCKET_NAME)
        yield boto3.resource('s3').Bucket(TEST_BUCKET_NAME)


@pytest.fixture
def dynamodb_table():
    with moto.mock_dynamodb2():
        boto3.client('dynamodb').create_table(
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'}
            ],
            TableName=TEST_DYNAMO_TABLE_NAME,
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            ProvisionedThroughput={
                'ReadCapacityUnits': 123,
                'WriteCapacityUnits': 123,
            },
        )
        yield boto3.resource('dynamodb').Table(TEST_DYNAMO_TABLE_NAME)


# @pytest.fixture()
# def dynamodb_table():
#     with mock_dynamodb2():
#         dynamodb_backend2.create_table("test-table", schema=[
#             {u'KeyType': u'HASH', u'AttributeName': u'name'}])
#         yield "test-table"

def test_handler(lambda_client, lambda_function, event):
    '''Test handler
    r = lambda_client.invoke(
        FunctionName=lambda_function,
        InvocationType='Event',
        Payload=json.dumps(event).encode()
    )

    lambda_return = r.get('Payload').read()
    slack_response = json.loads(lambda_return).get('slack_response')

    assert slack_response.get('ok') is True
    '''
    data_loader.handler(event, None)
