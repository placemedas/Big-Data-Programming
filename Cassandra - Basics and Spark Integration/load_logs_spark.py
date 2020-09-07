from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from cassandra.cluster import Cluster
import datetime
import sys
import re
import uuid


assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def log_etl(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    log_struct = line_re.findall(line)
    for each in log_struct:
        if each != '':
             uui = str(uuid.uuid4())
             date = datetime.datetime.strptime(each[1],'%d/%b/%Y:%H:%M:%S')
             yield each[0],date,each[2],int(each[3]),uui

def schema_def():
    df_schema = types.StructType([
        types.StructField('host',types.StringType()),
        types.StructField('datetime',types.TimestampType()),
        types.StructField('path',types.StringType()),
        types.StructField('bytes',types.IntegerType()),
        types.StructField('id',types.StringType())
    ])
    return df_schema

def main(input_dir, userid, table):
    in_text = sc.textFile(input_dir).repartition(160)
    log_in = in_text.flatMap(log_etl)
    nasalogs = schema_def()
    log_df = spark.createDataFrame(log_in,nasalogs)
    log_df.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=userid).save()

if __name__ == '__main__':
    conf = SparkConf().setAppName('nasa logs spark')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('nasa logs spark1').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    input_dir = sys.argv[1]
    userid = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, userid, table)

