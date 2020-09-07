from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def json_parse(line):
    score = float(line["score"])
    count = float(1)
    yield line["subreddit"],(count,score)

def add_pairs(x,y):
    sumcount = x[0] + y[0]
    sumscore = x[1] + y[1]
    return sumcount,sumscore

def average_score(x):
    reddit_name = x[0]
    reddit_avg = float (x[1][1]/x[1][0])
    if reddit_avg > 0:
        return reddit_name,reddit_avg


def relative_score(x):
    avg_score = x[1][0]
    comment_data = x[1][1]
    comm_score = comment_data['score']
    comm_auth = comment_data['author']
    rel_score = comm_score/avg_score
    yield rel_score,comm_auth

def get_key(kv):
    return kv[0]

def main(inputs, output):
    # main logic starts here
    # For calculating average score
    in_text = sc.textFile(inputs)
    comments = in_text.map(lambda x : json.loads(x)).cache()
    in_json = comments.flatMap(json_parse)
    out_sum = in_json.reduceByKey(add_pairs)
    out_avg = out_sum.map(average_score)
    # For calculating relative score
    commentbysub = comments.map(lambda c: (c['subreddit'],c))
    comments_join = out_avg.join(commentbysub)
    comment_auth = comments_join.flatMap(relative_score)
    comment_outdata = comment_auth.sortBy(get_key,ascending=False)
    comment_outdata.saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)