from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys

def main(keyspace, table):
    log_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    #log_df.show()
    log_df.registerTempTable("log_df")
    log_group = spark.sql("select count(host) as x,"
                          "power(count(host),2) as x2,"
                          "sum(bytes) as y,power(sum(bytes),2) as y2,"
                          "(count(host) * sum(bytes)) as xy "
                          "from log_df "
                          "group by host")
    log_group.registerTempTable("log_group")
    log_sum = spark.sql("select sum(1) as n,"
                        "sum(x) as x, "
                        "sum(x2) as x2, "
                        "sum(y) as y, "
                        "sum(y2) as y2,"
                        "sum(xy) as xy "
                        "from log_group")
    log_sum.show()
    log_sum.registerTempTable("log_sum")
    log_r = spark.sql("select ((n * xy) - (x * y)) / ((sqrt((n * x2) - pow(x,2))) * (sqrt((n * y2) - pow(y,2)))) as r "
                      "from log_sum")
    log_r.registerTempTable("log_r")
    log_corr = spark.sql("select r,"
                         "pow(r,2) as r2 "
                         "from log_r")
    log_corr.show()

if __name__ == '__main__':
        conf = SparkConf().setAppName('correlate logs')
        sc = SparkContext(conf=conf)
        cluster_seeds = ['199.60.17.32', '199.60.17.65']
        spark = SparkSession.builder.appName('nasa logs spark1').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
        sc.setLogLevel('WARN')
        assert sc.version >= '2.4'  # make sure we have Spark 2.4+
        assert sys.version_info >= (3, 5)
        keyspace = sys.argv[1]
        table = sys.argv[2]
        main(keyspace, table)

