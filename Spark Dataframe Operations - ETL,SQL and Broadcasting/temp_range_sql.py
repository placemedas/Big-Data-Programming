import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types,Row

spark = SparkSession.builder.appName('Temp Range SQL').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

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
    weather.registerTempTable("Wthr")
    wth_min = spark.sql("select w.station,w.date,w.value as tmin from Wthr as w where observation = 'TMIN' and isnull(qflag)")
    wth_min.registerTempTable("wthmin")
    wth_max = spark.sql("select h.station,h.date,h.value as tmax from Wthr as h where observation = 'TMAX' and isnull(qflag)")
    wth_max.registerTempTable("wthmax")
    wth_range = spark.sql("select mx.station,mx.date,((mx.tmax-mn.tmin)/10) as range from wthmin as mn join wthmax as mx on mn.station = mx.station and mn.date = mx.date")
    wth_range.registerTempTable("wth_range")
    wth_datemax = spark.sql("select rn.date,max(rn.range) as maxrng from wth_range as rn group by rn.date")
    wth_datemax.registerTempTable("wth_datemax")
    wth_select = spark.sql("select rnx.date,rnx.station,rnx.range from wth_range as rnx join wth_datemax as dx on rnx.date = dx.date and rnx.range = dx.maxrng order by rnx.date,rnx.station")
    wth_select.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

