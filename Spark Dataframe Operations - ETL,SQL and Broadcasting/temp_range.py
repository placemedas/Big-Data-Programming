import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('Temp Range').getOrCreate()
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
    weather_filter1 = weather.where((weather['qflag'].isNull())).cache()
    wth_tmax = weather_filter1.where((weather['observation'] == functions.lit('TMAX')))
    wth_tmin = weather_filter1.where((weather['observation'] == functions.lit('TMIN')))
    # Join TMIN and TMAX to get range
    range_cond = [wth_tmax['station'] == wth_tmin['station'],wth_tmax['date'] == wth_tmin['date']]
    wth_range = (wth_tmax.alias('tmax')).join((wth_tmin.alias('tmin')),range_cond,'inner').select('tmax.date','tmax.station',((functions.col('tmax.value') - functions.col('tmin.value'))/10).alias('range')).cache()
    # Obtain maximum of range
    wth_max = wth_range.groupBy('date').max('range')
    # Select only those ranges that have maximum values
    maxcond = [wth_range['date'] == wth_max['date'],wth_range['range'] == wth_max['max(range)']]
    wth_maxbydate = wth_range.join(wth_max,maxcond,'inner').select(wth_range['date'],wth_range['station'],wth_range['range'])
    # Sort the final output based on date and station
    wth_outdata = wth_maxbydate.orderBy(['date','station'])
    wth_outdata.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
