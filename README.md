# AWS Lambda Loader service

## About

This is a project that creates a service to load feed data from S3 to Snowflake.

An AWS lambda function is used which is running python3 code. 

This virtual env uses virtualenv to setup the dependencies. These dependencies are mentioned in requirements.txt.


## Prerequisites 

### Developers Computer

The python in this repository runs under Python 3.6

Start by running the unit tests usoing the following command:
nose2

Get unit test code coverage using the command:
nose2 --with-coverage
