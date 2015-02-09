__author__ = 'thawes'

from re import compile

DEBUG = True

from DynamoDbBackup import DynamoDbBackup

pattern = compile('WFM_*')
s3BucketName = 'dev-inin-dynamodbbackup'
table = 'ADevOpsTable'

dyn = DynamoDbBackup(pattern, s3BucketName, DEBUG)

dyn.__readTableData__()
# desc = dyn.descFromName(table)

#dyn.printTableData(desc)

# dyn.setReadThroughput(desc, 20)

# dyn.generateHive()

dyn.close()
