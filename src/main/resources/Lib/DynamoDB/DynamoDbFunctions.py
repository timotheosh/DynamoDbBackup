__author__ = 'thawes'

from com.inin.purecloud.devops.util import AuthToken
from com.amazonaws.services.dynamodbv2 import AmazonDynamoDBClient
from com.amazonaws.services.dynamodbv2.model import DescribeTableRequest
from com.amazonaws.services.dynamodbv2.model import ProvisionedThroughput
from re import match

class DynamoDbFunctions:
    def __init__(self, pattern, table=None, debug=False):
        """
        Constructor for class that will retrieve table data/descriptions and
        assign to class variables.
        :param pattern:
        :param table:
        :param debug:
        :return:
        """
        if debug:
            a = AuthToken('profile dev')
            creds = a.getToken()
            self.dynamoDb = AmazonDynamoDBClient(creds)
        else:
            self.dynamoDb = AmazonDynamoDBClient()
        self.tableDescriptions = []
        self.tableData = {}

        self.__getTableData__(pattern, table)

    def __getTableData__(self, pattern, table=None):
        """
        Function to retrieve dynamo table descriptions, and assign to class
        variables.
        :param table: Get data description(s) just for this table name.
                      Otherwise, it retrieves a list of descriptions for all
                      tables in the account.
        :return:      void
        """
        tables = self.dynamoDb.listTables()

        tableDescriptions = []
        tableData = {}
        for tableName in tables.tableNames:
            treq = None
            if table and table == tableName:
                treq = DescribeTableRequest().withTableName(tableName)
            elif not pattern:
                treq = DescribeTableRequest().withTableName(tableName)
            elif match(pattern, tableName):
                treq = DescribeTableRequest().withTableName(tableName)
            else:
                pass

            if treq:
                desc = self.dynamoDb.describeTable(treq).getTable()
                throughPut = desc.getProvisionedThroughput()
                tableData.update({desc.getTableName():
                                      {'size': desc.getTableSizeBytes(),
                                       'read': throughPut.readCapacityUnits,
                                       'write': throughPut.writeCapacityUnits}})
                tableDescriptions.append(desc)

        self.tableDescriptions = tableDescriptions
        self.tableData = tableData

    def getTableList(self):
        """
        Generates a list of table names from the TableDescrion objects.
        :return: python list of table names.
        """
        rtn = []
        for table in self.tableDescriptions:
            rtn.append(table.tableName)
        return rtn

    def descFromName(self, tableName):
        """
        Returns the java TableDescription object associated with the given
        tableName.
        :param tableName: String containing the name of the table to get the
                          description for.
        :return: java com.amazonaws.services.dynamodbv2.model.TableDescription
                 object
        """
        rtn = None
        for x in self.tableDescriptions:
            if x.tableName == tableName:
                rtn = x
        return rtn

    def setReadThroughput(self, tableDesc, throughput):
        """
        Sets the specified throughput capacity for the specified table.
        :param tableDesc: com.amazonaws.services.dynamodbv2.model.TableDescription Object
        :param throughput: A simple long with the numher of read units per second to change to.
        :return: void
        """
        if throughput > 500:
            print "throughput of reads is set too high: %s" % throughput
        elif throughput < 1:
            print "throughput of reads must be at least 1"

        else:
            try:
                reads = tableDesc.getProvisionedThroughput().getReadCapacityUnits()
                writes = tableDesc.getProvisionedThroughput().getWriteCapacityUnits()
                p = ProvisionedThroughput(throughput, writes)
                if reads == throughput:
                    print "The specified reads capacity is already set to %s" % reads
                else:
                    self.dynamoDb.updateTable(tableDesc.getTableName(), p)
            except Exception,e:
                print e.message

    def printTableData(self, tableDesc):
        """
        Prints Table Data, given the Java TableDescription object.
        :param tableDesc: com.amazonaws.services.dynamodbv2.model.TableDescription Object
        :return: void
        """
        try:
            print "Table Name: %s" % tableDesc.getTableName()
            print "Table Read  Throughput: %s" % tableDesc.getProvisionedThroughput().getReadCapacityUnits()
            print "Table Write Throughput: %s" % tableDesc.getProvisionedThroughput().getWriteCapacityUnits()
            print "Table Size: %s" % tableDesc.getTableSizeBytes()
            print tableDesc.getProvisionedThroughput()
            print ""
        except Exception,e:
            print "Exception in printTableData: %s" % e.args
            print e.message

    def close(self):
        self.dynamoDb.shutdown()