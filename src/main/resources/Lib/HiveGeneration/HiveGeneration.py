__author__ = 'thawes'

from com.inin.purecloud.devops.util import Conversions
from org.rythmengine import RythmEngine

class HiveGeneration:
    def __init__(self, tables, s3BucketName, s3path, readPercent):
        """
        Returns a string containing the Hive script to be executed on the list
        of tables, inserting the appropriate S3 information that must also be
        passed to this module.
        :param tables: python list of table names to backup
        :param s3BucketName: Name of the S3 Bucket to store the backup
        :param s3path: S3 path (or "key", as it is also often referred as, where
                       the table backup will be stored.
        """
        self.tables = tables
        self.s3BucketName = s3BucketName
        self.s3path = s3path
        self.scriptData = self.__generateHive__(tables, s3BucketName, s3path, readPercent)

    def __generateHive__(self, tables, s3BucketName, s3path, readPercent):
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

        hscript = []
        MyClass = Conversions()
        istr = MyClass.getClass().getClassLoader().getResourceAsStream('templates/hive.tpl')
        template = MyClass.convertStreamToString(istr)
        engine = RythmEngine({})


        for table in tables:
            params = {"tableName": table,
                      "s3Bucket": s3BucketName,
                      "s3Path": s3path,
                      "readPercent": float(readPercent)}
            hscript.append(engine.render(template, params))
        return hscript

    def dumpScript(self):
        return "\n".join(self.scriptData)