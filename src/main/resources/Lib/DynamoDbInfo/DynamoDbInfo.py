__author__ = 'thawes'
DEBUG = True

from com.amazonaws.regions import Region, Regions
from com.amazonaws.auth import BasicSessionCredentials
from com.amazonaws.services.dynamodbv2 import AmazonDynamoDBClient
from com.amazonaws.services.dynamodbv2.model import DescribeTableRequest

theRegion = Region.getRegion(Regions.US_EAST_1)

class DynamoDbInfo:
    def __init__(self):
        pass

    def getData(self):
        if DEBUG:
            from org.ini4j import Ini
            from os import environ
            from java.io import FileReader

            profile = 'profile dev'
            configfile = '%s/.aws/config' % environ['HOME']
            ini = Ini(FileReader(configfile))

            access_key = ini.get(profile).fetch('aws_access_key_id')
            secret_key = ini.get(profile).fetch('aws_secret_access_key')
            session_token = ini.get(profile).fetch('aws_session_token')

            creds = BasicSessionCredentials(access_key,
                                            secret_key,
                                            session_token);
            dynamoDb = AmazonDynamoDBClient(creds)
        else:
            dynamoDb = AmazonDynamoDBClient()

        tables = dynamoDb.listTables()

        tableDescriptions = []
        for tableName in tables.tableNames:
            treq = DescribeTableRequest().withTableName(tableName)
            tableDescriptions.append(dynamoDb.describeTable(treq).getTable())

        return tableDescriptions
