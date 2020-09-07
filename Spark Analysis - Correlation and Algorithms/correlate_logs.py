from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import re

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def log_etl(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    log_struct = line_re.findall(line)
    for each in log_struct:
        if each != '':
             yield each[0],int(each[3])

def schema_def():
    df_schema = types.StructType([
        types.StructField('hostname',types.StringType()),
        types.StructField('bytes',types.IntegerType())
    ])
    return df_schema

def main(inputs, output):
    in_text = sc.textFile(inputs)
    log_in = in_text.flatMap(log_etl)
    nasa_schema = schema_def()
    log_df = spark.createDataFrame(log_in,nasa_schema)
    log_df.registerTempTable("log_df")
    log_group = spark.sql("select count(hostname) as x,"
                          "power(count(hostname),2) as x2,"
                          "sum(bytes) as y,power(sum(bytes),2) as y2,"
                          "(count(hostname) * sum(bytes)) as xy "
                          "from log_df "
                          "group by hostname")
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
    sc.setLogLevel('WARN')
    spark = SparkSession.builder.appName('correlate logs1').getOrCreate()
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)








