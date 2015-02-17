__author__ = 'thawes'

from re import compile

DEBUG = True

from DynamoDB import DynamoDbFunctions
from S3Storage import S3Functions
from HiveGeneration import HiveGeneration
from DynamoDbBackup import PreBackup

pattern = compile('WFM_*')
s3BucketName = 'dev-inin-dynamodbbackup'

dynamoDb = DynamoDbFunctions(pattern, None, DEBUG)
s3Storage = S3Functions(s3BucketName, DEBUG)

hive = HiveGeneration(dynamoDb.getTableList(),
                      s3Storage.s3BucketName,
                      s3Storage.s3path, 0.15)

s3Storage.writeHiveData(hive.dumpScript())
