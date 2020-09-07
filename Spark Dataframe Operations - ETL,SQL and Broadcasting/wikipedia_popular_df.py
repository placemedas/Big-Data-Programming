import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('wikipedia pagecount df').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary
def schema_def():
    wiki_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('pagecount', types.LongType()),
        types.StructField('pagesize', types.LongType()),
    ])
    return wiki_schema

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    file_path = path.split("/")[-1]
    pagedate = file_path.split("-")[-2]
    pagetime = file_path.split("-")[-1]
    pagehour = str(pagedate) + str("-") + str(pagetime[0:2])
    return pagehour


def main(inputs, output):
    wikipedia_schema = schema_def()
    pagedata = spark.read.csv(inputs,schema=wikipedia_schema,sep=' ').withColumn('hour',path_to_hour(functions.input_file_name()))
    page_filter = pagedata.where((pagedata['language'] == 'en') & (pagedata['title'] != 'Main_Page') & (~pagedata['title'].startswith('Special:'))).cache()
    maxcount = page_filter.groupby('hour').max('pagecount')
    joincond = [page_filter["pagecount"] == maxcount['max(pagecount)'],page_filter["hour"] == maxcount['hour']]
    pagebest = page_filter.join(maxcount.hint("broadcast"),joincond,'inner').select(page_filter['hour'],page_filter['title'],(page_filter['pagecount'].alias('views')))
    pageout = pagebest.orderBy(['hour','title'])
    pageout.write.json(output, mode='overwrite')
    pageout.explain()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)