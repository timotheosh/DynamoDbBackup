__author__ = 'thawes'

DEBUG = True

from DynamoDbBackup import DynamoDbBackup

dyn = DynamoDbBackup(DEBUG)

t1 = dyn.getData('ADevOpsTable')

dyn.tableData(t1[0])

dyn.setReadThroughput(t1[0], 20)

dyn.generateHive()

dyn.close()
