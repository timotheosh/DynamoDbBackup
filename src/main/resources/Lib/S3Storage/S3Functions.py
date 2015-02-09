__author__ = 'thawes'

from com.inin.purecloud.devops.util import Conversions, AuthToken
from com.amazonaws.services.s3 import AmazonS3Client
from com.amazonaws.services.s3.model import PutObjectRequest
from com.amazonaws.services.s3.model import ObjectMetadata, AmazonS3Exception
from datetime import datetime
from json import dumps, loads

class S3Functions:
    def __init__(self, s3BucketName, debug=False):
        if debug:
            auth = AuthToken('profile dev')
            self.s3client = AmazonS3Client(auth.getToken())
        else:
            self.s3client = AmazonS3Client()

        self.s3BucketName = s3BucketName
        now = datetime.now()
        self.date = now.date().isoformat()
        self.s3path = "Backups/%s" % self.date

    def writeTableData(self, tableData):
        """
        Given a python dict object (tableData) this will generate a single json
        file, and write it to
        s3://self.s3BucketName/self.s3path/backup-data.json
        :param tableData: Python dict object with the table data.
        """
        data = dumps(tableData)
        self.writeData(data, 'application/json',
                       'backup-data.json', None,
                       dynamodbbackup=self.date)

    def writeHiveData(self, hivescript):
        self.writeData(hivescript, 'application/x-hive',
                       'dynamodb-backup.hive', 'HiveScripts')

    def writeData(self, data, datatype, filename, s3key=None, **kwargs):
        """
        Java AmazonS3Client.putObject() requires either a java.io.File
        or a java.io.InputStream. InputStream is an Abstractclass, so we need
        a class that inherits from it.
        """
        try:
            MyClass = Conversions()
            # Convert data to an InputStream
            payload = MyClass.convertStringToStream(data)
            meta = ObjectMetadata()
            meta.setContentLength(len(data))
            meta.setContentType(datatype)
            if not s3key:
                s3key = self.s3path
            for x in kwargs.viewkeys():
                meta.addUserMetadata(x, kwargs[x])
            self.s3client.putObject(
                PutObjectRequest(self.s3BucketName,
                                 "%s/%s" % (s3key, filename),
                                 payload, meta))
        except Exception,e:
            print "Error in __writeTableDescData__()!"
            print e.args
            print e.message

    def readTableData(self):
        data = None
        try:
            MyClass = Conversions()
            jsonFile = "%s/backup-data.json" % self.s3path
            istr = self.s3client.getObject(self.s3BucketName, jsonFile).getObjectContent()
            data = loads(MyClass.convertStreamToString(istr))
            # Data is now a python dict.
        except AmazonS3Exception,ae:
            data = ae.getAdditionalDetails()
            print ae.toString()
        except Exception,e:
            print e.message

        return data