from pyspark import SparkConf, SparkContext
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import json


def json_parse(line):
    comment = json.loads(line)
    name = comment["subreddit"]
    score = float(comment["score"])
    author = comment["author"]
    if "e" in name:
        yield name, score, author

def score_post(x):
    if x[1] > 0:
        return x

def score_negt(x):
    if x[1] <= 0:
        return x

def get_key(kv):
    return kv[0]


def main(inputs, output):
    # main logic starts here
    in_text = sc.textFile(inputs)
    comment_map = in_text.flatMap(json_parse).cache()
    comment_pos = comment_map.filter(score_post)
    comment_neg = comment_map.filter(score_negt)
    comment_pos.sortBy(get_key).map(json.dumps).saveAsTextFile(output + '/positive')
    comment_neg.sortBy(get_key).map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
