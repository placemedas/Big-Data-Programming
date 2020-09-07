from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def json_parse(line):
    json_in = json.loads(line)
    score = float(json_in["score"])
    count = float(1)
    yield json_in["subreddit"],(count,score)

def add_pairs(x,y):
    sumcount = x[0] + y[0]
    sumscore = x[1] + y[1]
    return sumcount,sumscore

def average_score(x):
    reddit_name = x[0]
    reddit_avg = float (x[1][1]/x[1][0])
    return reddit_name,reddit_avg

def get_key(kv):
    return kv[0]

def main(inputs, output):
    # main logic starts here
    in_text = sc.textFile(inputs)
    in_json = in_text.flatMap(json_parse)
    out_sum = in_json.reduceByKey(add_pairs)
    out_avg = out_sum.map(average_score)
    outdata_sort = out_avg.sortBy(get_key).map(json.dumps)
    outdata_sort.saveAsTextFile(output)
#    with open(output, 'w') as out:
#        out.write(outdata)



if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)