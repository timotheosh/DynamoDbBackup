__author__ = 'thawes'

from re import compile

DEBUG = True

from DynamoDB import DynamoDbFunctions
from S3Storage import S3Functions
from HiveGeneration import HiveGeneration
from DynamoDbBackup import PreBackup

pattern = compile('WFM_*')
s3BucketName = 'dev-inin-dynamodbbackup'
#table = 'ADevOpsTable'

"""
Step 1  Get current DynamoDb configurations, and save as a json document in S3
"""
dynamoDb = DynamoDbFunctions(pattern, None, DEBUG)
s3Storage = S3Functions(s3BucketName, DEBUG)

s3Storage.writeTableData(dynamoDb.tableData)

"""
Step 2  Increase the read throughput of the Dynamo tables to be backed up.
"""
for tableDesc in dynamoDb.tableDescriptions:
    dynamoDb.setReadThroughput(tableDesc, 20)

"""
Step 3  Generate the Hive script and save on S3 for EMR to pick up and execute.
"""

hive = HiveGeneration(dynamoDb.getTableList(),
                      s3Storage.s3BucketName,
                      s3Storage.s3path, 10)

s3Storage.writeHiveData(hive.dumpScript())


#dyn = PreBackup(pattern, None, DEBUG)
#dyn.__readTableData__()
# desc = dyn.descFromName(table)

#dyn.printTableData(desc)

# dyn.setReadThroughput(desc, 20)

# dyn.generateHive()

#dyn.close()
