
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types


def main(topic):
        messages = spark.readStream.format('kafka') \
                        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
                        .option('subscribe', topic).load()
        values = messages.select(messages['value'].cast('string'))
        values_sp = values.withColumn("x", functions.split(values["value"], " ").getItem(0)) \
                                .withColumn("y", functions.split(values["value"], " ").getItem(1))
        # Calculation of Slope is as below
        values_sp.registerTempTable("values_sp")
        values_cal = spark.sql("select sum(x*y) as sumxy,count(*) as n,sum(x) as sumx, sum(y) as sumy, sum(pow(x,2)) as sumx2, pow(sum(x),2) as sum2x from values_sp")
        values_cal.registerTempTable("values_cal")
        values_slope = spark.sql("select ((sumxy - ((sumx * sumy) / n)) / (sumx2 - (sum2x / n))) as slope, (sumy / n) as sumyn, (sumx / n) as sumxn from values_cal")

        #Calculation of intercept
        values_slope.registerTempTable("values_slope")
        values_intercept = spark.sql("select slope, (sumyn - (slope * sumxn)) as inercept from values_slope")

        stream = values_intercept.writeStream.outputMode("complete").format("console").start()
        #values.printSchema()
        stream.awaitTermination(600)



if __name__ == '__main__':
        conf = SparkConf().setAppName('read_stream')
        sc = SparkContext(conf=conf)
        spark = SparkSession.builder.appName('read stream').getOrCreate()
        sc.setLogLevel('WARN')
        assert sc.version >= '2.4'  # make sure we have Spark 2.4+
        assert sys.version_info >= (3, 5)
        topic = sys.argv[1]
        main(topic)

