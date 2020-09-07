from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+



def words_once(line):
    # regex incorporation to split input data
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    # splitting the data and converting to lower case
    for w in wordsep.split(line.lower()):
            yield (w, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

# new function created to check spaces and filter them
def checkspace(key):
        return '' != key[0]

def main(inputs, output):

    text = sc.textFile(inputs)
    words_map = text.repartition(80)
    words = words_map.flatMap(words_once)

    # Filter has been applied to remove spaces
    wordsamp = words.filter(checkspace)
#    print(wordsamp.getNumPartitions())
    wordcount_par = wordsamp.reduceByKey(add)

    outdata = wordcount_par.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('word count')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)




