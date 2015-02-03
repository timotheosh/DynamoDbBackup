__author__ = 'thawes'

from com.inin.purecloud.devops import InitJython
from org.rythmengine import RythmEngine
from com.amazonaws.auth import BasicSessionCredentials
from com.amazonaws.services.dynamodbv2 import AmazonDynamoDBClient
from com.amazonaws.services.dynamodbv2.model import DescribeTableRequest
from com.amazonaws.services.dynamodbv2.model import ProvisionedThroughput
from java.io import File
from java.lang import String

class DynamoDbBackup:
    """
    Class for backing up DynamoDB Tables
    """
    def __init__(self, debug=False):
        if debug:
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
                                            session_token)
            self.dynamoDb = AmazonDynamoDBClient(creds)
        else:
            self.dynamoDb = AmazonDynamoDBClient()

    def getData(self, table=None):
        """
        Function to retrieve dynamo table descriptions.
        :param table: Return description(s) just for this table name. Otherwise,
                      it returns a list of descriptions for all tables in the
                      account.
        :return:      Python list of java
                      com.amazonaws.services.dynamodbv2.model.TableDescription
                      objects. If you specify the table param above, it still
                      returns a Python list with just that table desction in it.
        """
        tables = self.dynamoDb.listTables()

        tableDescriptions = []
        for tableName in tables.tableNames:
            if table and table == tableName:
                treq = DescribeTableRequest().withTableName(tableName)
                tableDescriptions.append(self.dynamoDb.describeTable(treq).getTable())
            else:
                treq = DescribeTableRequest().withTableName(tableName)
                tableDescriptions.append(self.dynamoDb.describeTable(treq).getTable())

        return tableDescriptions

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

    def tableData(self, tableDesc):
        """
        Returns Table Data
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
            print "Exception in ProcessInfo: %s" % e.args
            print e.message

    def generateHive(self, table = None):
        """
        Method for creating the Hive script.
        :param table:
        :return:
        """

        """
        Have to get the template file in relation to the class. So we grab a
        class in our jar, get the resource as an InputStream, use a handy
        function in our class that converts InputStream to a Java String

        """
        MyClass = InitJython([''])
        istr = MyClass.class.getClassLoader().getResourceAsStream('templates/hive.tpl')
        template = MyClass.convertStreamToString(istr)

        tables = self.dynamoDb.listTables()
        engine = RythmEngine({})

        for tableName in tables.tableNames:
            params = {"tableName": tableName}
            out = engine.render(template, params)
            print "For table: %s\n%s" % (tableName, out)

    def close(self):
        self.dynamoDb.shutdown()