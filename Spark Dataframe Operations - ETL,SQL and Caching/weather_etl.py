import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


# add more functions as necessary
def schema_def():
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])
    return observation_schema


def main(inputs, output):
    weather_schema = schema_def()
    weather = spark.read.csv(inputs, schema=weather_schema)
    # Filters are applied below
    weather_filter1 = weather.where((weather['qflag'].isNull()) & (weather['station'].startswith('CA')))
    weather_filter2 = weather_filter1.where(weather['observation'] == functions.lit('TMAX'))
    # Selection of output is as below
    weather_output = weather_filter2.select(weather['station'], weather['date'], (weather['value'] / 10).alias('tmax'))
    weather_output.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
