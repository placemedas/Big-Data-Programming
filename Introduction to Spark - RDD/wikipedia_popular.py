import sys
import re
import collections
from pyspark import SparkConf, SparkContext

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def pages_count(line):
    pagesep = re.compile(re.escape(' '))
    pagecols = pagesep.split(line)
    pagedate = pagecols[0]
    pagelang = pagecols[1]
    pagetitle = pagecols[2]
    pagecount = int(pagecols[3])

    yield pagedate, pagelang, pagetitle, pagecount


def checkcond(x):
    if x[1] == 'en' and x[2] != "Main_Page" and not x[2].startswith("Special:"):
        return x


def maximum_count(x, y):
     return max(x, y,key=lambda x: x[0])

def get_key(kv):
    return kv[0]


def tab_separated(kv):
    return '%s \t %s' % (kv[0], kv[1])


text = sc.textFile(inputs)
pagedet = text.flatMap(pages_count)
pagefil = pagedet.filter(checkcond)
pagedet_pair = pagefil.map(lambda x: (x[0], (x[3], x[2])))
#print(pagedet_pair.take(10))

maxviews = pagedet_pair.reduceByKey(maximum_count,numPartitions=2)
#print(maxviews.take(10))

outdata = maxviews.sortBy(get_key).map(tab_separated)
#print(outdata.take(10))

outdata.saveAsTextFile(output)
