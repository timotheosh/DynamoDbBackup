__author__ = 'thawes'

from DynamoDbInfo import DynamoDbInfo

dyn = DynamoDbInfo()
tl = dyn.getData()
print dir(tl[0])
