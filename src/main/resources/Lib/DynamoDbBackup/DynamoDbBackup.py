__author__ = 'thawes'

from com.inin.purecloud.devops.util import Conversions
from org.rythmengine import RythmEngine
from com.amazonaws.auth import BasicSessionCredentials
from com.amazonaws.services.dynamodbv2 import AmazonDynamoDBClient
from com.amazonaws.services.dynamodbv2.model import DescribeTableRequest
from com.amazonaws.services.dynamodbv2.model import ProvisionedThroughput
from com.amazonaws.services.s3 import AmazonS3Client
from com.amazonaws.services.s3.model import PutObjectRequest
from com.amazonaws.services.s3.model import ObjectMetadata, AmazonS3Exception
from datetime import datetime
from re import match
from json import dumps, loads

class DynamoDbBackup:
    """
    Class for backing up DynamoDB Tables.
    This does not actually do the back up process. It initializes the DynamoDb
    tables, incerasing the read throughputs in preparation of backing up the
    tables using AWS EMR with Hive scripts.

    This class also generates the Hive scripts for EMR to run.

    After EMR backs up the Dynamo tables, this class will be called again to
    restore the DYnamoDb read throughput settings to what they were originally.

    Pre Backup steps:

      Step 1.  Collect the throughput data and size of every DynamoDb tabble to
               be backed up, and store in a json file in an S3 bucket (in the
               same directory that the backup files will be placed.

      Step 2.  Change the read throughput for each table based on the table
               size.

      Step 3.  Create the Hive script to be used to back up the dynamodb tables.

    Post Backup steps:

      Step 1.  Retrieve the stored json file in S3, and read.

      Step 2.  Restore read throughput values back to their original values
               based on the information stored in the read json file.

    """
    def __init__(self, pattern, s3BucketName, debug=False, table=None):
        """

        :param debug: When debug = true, AWS credentials will be retrieved from
                      the user's $AWS_CONFIG_FILE or $HOME/.aws/config file, if
                      $AWS_CONFIG_FILE is not defined.
        :param table: Usually for testing, this will perform operations only on
                      the specified table.
        :return: void
        """
        if debug:
            from org.ini4j import Ini
            from os import environ
            from java.io import FileReader

            profile = 'profile dev'
            if environ.has_key('AWS_CONFIG_FILE'):
                configfile = environ['AWS_CONFIG_FILE']
            else:
                configfile = '%s/.aws/config' % environ['HOME']
            ini = Ini(FileReader(configfile))

            access_key = ini.get(profile).fetch('aws_access_key_id')
            secret_key = ini.get(profile).fetch('aws_secret_access_key')
            session_token = ini.get(profile).fetch('aws_session_token')

            creds = BasicSessionCredentials(access_key,
                                            secret_key,
                                            session_token)
            self.creds = creds
            self.dynamoDb = AmazonDynamoDBClient(self.creds)
        else:
            self.creds = None
            self.dynamoDb = AmazonDynamoDBClient()

        # S3 path is created from today's date
        now = datetime.now()
        self.date = now.date().isoformat()
        self.s3path = "Backups/%s" % self.date

        self.s3BucketName = s3BucketName
        self.tables = []
        self.tableData = {}
        self.__getTableList__(pattern, table)
        self.throughput = 15.0

    def __getTableList__(self, pattern, table=None):
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

        self.tables = tableDescriptions
        self.tableData = tableData
        self.__writeTableData__()

    def __writeTableData__(self):
        if self.creds:
            s3client = AmazonS3Client(self.creds)
        else:
            s3client = AmazonS3Client()
        """
        Java AmazonS3Client.putObject() requires either a java.io.File
        or a java.io.InputStream. InputStream is an Abstractclass, so we need
        a class that inherits from it.
        """
        try:
            MyClass = Conversions()
            # Convert python dict to json, then convert it to an InputStream
            data = dumps(self.tableData)
            payload = MyClass.convertStringToStream(data)
            meta = ObjectMetadata()
            meta.setContentLength(len(data))
            meta.setContentType('application/json')
            meta.addUserMetadata('dynamodbbackup', self.date)
            s3client.putObject(
                PutObjectRequest(self.s3BucketName,
                                 "%s/backup-data.json" % self.s3path,
                                 payload, meta))
        except Exception,e:
            print "Error in __writeTableDescData__()!"
            print e.args
            print e.message

    def __readTableData__(self):
        if self.creds:
            s3client = AmazonS3Client(self.creds)
        else:
            s3client = AmazonS3Client()
        try:
            MyClass = Conversions()
            jsonFile = "%s/backup-data.json" % self.s3path
            istr = s3client.getObject(self.s3BucketName, jsonFile).getObjectContent()
            data = loads(MyClass.convertStreamToString(istr))
            # Data is now a python dict.
            # TODO: Create method that will take dict and restore the read throughput limits to thier original values.
            print data
        except AmazonS3Exception,ae:
            data = ae.getAdditionalDetails()
            print ae.toString()
        except Exception,e:
            print e.message

    def descFromName(self, tableName):
        rtn = None
        for x in self.tables:
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
            print "Exception in printTableData: %s" % e.args
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

        MyClass = Conversions()
        istr = MyClass.getClass().getClassLoader().getResourceAsStream('templates/hive.tpl')
        template = MyClass.convertStreamToString(istr)
        engine = RythmEngine({})


        for table in self.tables:
            params = {"tableName": table.tableName,
                      "s3Bucket": "%s/Backups" % self.s3BucketName,
                      "s3Path": self.s3path,
                      "readPercent": 1.0}
            out = engine.render(template, params)
            print "For table: %s\n%s" % (table.tableName, out)

    def close(self):
        self.dynamoDb.shutdown()