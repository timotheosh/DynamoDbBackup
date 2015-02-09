__author__ = 'thawes'

from DynamoDB import DynamoDbFunctions

class PreBackup:
    """
    Class for preparing backing up DynamoDB Tables by an EMR instance.
    This does not actually do the back up process. It initializes the DynamoDb
    tables, increasing the read throughputs in preparation of backing up the
    tables using AWS EMR with Hive scripts.

    Pre Backup steps:

      Step 1.  Collect the throughput data and size of every DynamoDb table to
               be backed up, and store in a json file in an S3 bucket (in the
               same directory that the backup files will be placed.

      Step 2.  Change the read throughput for each table based on the table
               size.
    """
    def __init__(self, pattern, s3BucketName, debug=False):
        """

        :param pattern:
        :param s3BucketName:
        :param debug:
        :return:
        """
        self.pattern = pattern
        self.s3BucketName = s3BucketName
        self.debug = debug

    def saveDynamoDbData(self):
        d = DynamoDbFunctions(pattern, None, debug)
        s = S3Functions()
