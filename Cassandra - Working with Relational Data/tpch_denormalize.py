from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from cassandra.cluster import Cluster
import sys

def main(in_keyspace, out_keyspace):
        table1 = "orders"
        table2 = "lineitem"
        table3 = "part"
        orders = spark.read.format("org.apache.spark.sql.cassandra").options(table=table1, keyspace=in_keyspace).load()
        orders.registerTempTable("orders")
        lineitem = spark.read.format("org.apache.spark.sql.cassandra").options(table=table2, keyspace=in_keyspace).load()
        lineitem.registerTempTable("lineitem")
        part = spark.read.format("org.apache.spark.sql.cassandra").options(table=table3, keyspace=in_keyspace).load()
        part.registerTempTable("part")

        order_det = spark.sql("select o.orderkey,o.custkey,o.orderstatus,o.totalprice,o.orderdate,o.order_priority,o.clerk,o.ship_priority,o.comment,p.name as part_names "
                                "from orders o "
                                "join lineitem l on o.orderkey = l.orderkey "
                                "join part p on p.partkey = l.partkey")

        order_list = order_det.groupby('orderkey','custkey','orderstatus','totalprice','orderdate','order_priority','clerk','ship_priority','comment').agg(functions.collect_set('part_names').alias('part_names')).orderBy('orderkey')
        order_list.write.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=out_keyspace).save()
        print("Finished inserts")



if __name__ == '__main__':
    conf = SparkConf().setAppName('tpch denormalize')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('nasa logs spark1').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    in_keyspace = sys.argv[1]
    out_keyspace = sys.argv[2]
    main(in_keyspace, out_keyspace)


