from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys

def graph_etl(line):
    split1 = line.split(":")
    split2 = split1[1].split()
    for each in split2:
        yield int(split1[0]),int(each)

def schema_edge():
    df1_schema = types.StructType([
        types.StructField('source',types.IntegerType()),
        types.StructField('dest',types.IntegerType())
    ])
    return df1_schema

def schema_path():
    df2_schema = types.StructType([
        types.StructField('node',types.StringType()),
        types.StructField('source',types.IntegerType()),
        types.StructField('distance', types.IntegerType())
    ])
    return df2_schema

def Reverse(val):
    val.reverse()
    return val

def main(input, output,start,dest):
    # Reading input files
    graph_raw = sc.textFile(input)
    graph = graph_raw.flatMap(graph_etl)
    # Creation of dataframe with graph edges
    edge_schema = schema_edge()
    edges = spark.createDataFrame(graph,edge_schema).cache()
    edges.registerTempTable("edges")
    # Setting up origin node to get known paths
    origin_node = [(start,0,0)]
    # Creation of knownpath frame with origin node
    path_schema = schema_path()
    known_path = spark.createDataFrame(origin_node,path_schema)
    known_path.registerTempTable("known_path")
    flag = 'n'

    for i in range(6):
        # Creation of incremental paths
        path_loop = spark.sql("select e.dest as node,e.source,(p.distance + 1) as distance from edges e join known_path p on e.source = p.node order by e.dest")
        path_loop.registerTempTable("path_loop")
        # Get the node to check whether it matches with dest
        end_node = spark.sql("select node from path_loop where node ='{}'".format(dest))
        # Removing duplicate paths and combine with the rest of paths
        known_path_med = known_path.unionAll(path_loop).distinct().cache()
        # Keep only paths with minimum distance
        known_path_med.registerTempTable("known_path_med")
        known_path = spark.sql("select a.* from known_path_med a join (select node,min(distance) as mins from known_path_med group by node) b on a.node = b.node and a.distance = b.mins order by a.node")
        # Write the progress of path into output
        known_path.write.json(output + '/iter-' + str(i))
        known_path.registerTempTable("known_path")
        # Breakpoint to not progress if the destination node is found
        if end_node.count() > 0:
            flag = 'y'
            break
        else:
            flag = 'n'

    if flag == 'y':
        # Code to reconstruct the path
        dst = dest
        finaldest = [int(dest)]
        path_val = 0

        while path_val != int(start):
            path_source = spark.sql("select source from known_path where node = '{}'".format(dst))
            path_list = path_source.collect()
            path_val = path_list[0].source
            finaldest.append(path_val)
            dst = path_val

        outdata = Reverse(finaldest)
        finalpath = sc.parallelize(outdata)
        finalpath.saveAsTextFile(output + '/path')
        print(outdata)
    else:
        print("Destination not found in 6 iterations")


if __name__ == '__main__':
    conf = SparkConf().setAppName('optimal path')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession.builder.appName('optimal path1').getOrCreate()
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    dest = sys.argv[4]
    input = inputs + "/links-simple-sorted.txt"
    if start == dest:
        print("Source and Destination are same. No optimal path can be found")
    else:
        main(input, output,start,dest)

