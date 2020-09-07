from pyspark import SparkConf, SparkContext
import sys
import random

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def map_comp(samp):
    iter = 0
    random.seed()
    sum = float(0)
    while sum < 1:
       sum += random.random()
       iter = iter + 1
    return iter


def main(inputs):
    rang = sc.range(0, inputs, numSlices=8)
    rang_map = rang.map(map_comp)
    rang_red = rang_map.reduce(lambda x, y: x + y)
    print(rang_red / inputs)


if __name__ == '__main__':
    conf = SparkConf().setAppName("Euler's Constant")
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = int(sys.argv[1])
    main(inputs)
