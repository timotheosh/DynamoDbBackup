__author__ = 'thawes'

from com.inin.purecloud.devops.util import AuthToken
from com.amazonaws.services.dynamodbv2 import AmazonDynamoDBClient
from com.amazonaws.services.dynamodbv2.model import DescribeTableRequest
from com.amazonaws.services.dynamodbv2.model import ProvisionedThroughput
from re import match
from math import ceil

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
        Generates a list of table names from the TableDescription objects.
        :return: python list of table names.
        """
        rtn = []
        for table in self.tableDescriptions:
            rtn.append(table.tableName)
        return rtn
