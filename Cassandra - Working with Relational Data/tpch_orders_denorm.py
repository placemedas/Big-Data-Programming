from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys


def outputrdd(line):
        orderkey = line[0]
        totalprice = line[1]
        names = sorted(list(line[2]))
        namestr = ', '.join(names)
        return 'Order #%d $%.2f: %s' % (orderkey, totalprice, namestr)

def main(keyspace, outdir, orderkeys):
        table1 = "orders_parts"
        orders = spark.read.format("org.apache.spark.sql.cassandra").options(table=table1, keyspace=keyspace).load()
        for i in range(0, len(orderkeys)):
                orderkeys[i] = int(orderkeys[i])
        orderfilt = orders.where(orders["orderkey"].isin(orderkeys))
        orderfilt.registerTempTable("orders")
        order_det = spark.sql("select orderkey,totalprice,part_names from orders order by orderkey")
        rdd_out = order_det.rdd.map(outputrdd)
        rdd_out.saveAsTextFile(outdir)

if __name__ == '__main__':
        conf = SparkConf().setAppName('tpch_orders_df')
        sc = SparkContext(conf=conf)
        cluster_seeds = ['199.60.17.32', '199.60.17.65']

        spark = SparkSession.builder.appName('tpch orders').config('spark.cassandra.connection.host',','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
        sc.setLogLevel('WARN')
        assert sc.version >= '2.4'  # make sure we have Spark 2.4+
        assert sys.version_info >= (3, 5)
        keyspace = sys.argv[1]
        outdir = sys.argv[2]
        orderkeys = sys.argv[3:]
        main(keyspace, outdir, orderkeys)

