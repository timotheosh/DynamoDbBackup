@args String tableName, String s3Bucket, String s3Path, Double readPercent
CREATE EXTERNAL TABLE hive-@tableName (item map<string,string>)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "@tableName");

CREATE EXTERNAL TABLE @tableName (item map<string, string>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION 's3://@s3Bucket/@s3Path';

SET dynamodb.throughput.read.percent=@readPercent;

INSERT OVERWRITE TABLE @tableName SELECT *
FROM hive-@tableName;