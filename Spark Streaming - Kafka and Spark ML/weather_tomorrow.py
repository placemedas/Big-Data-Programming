import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def tmrw_model(model_file):
    # get the data
    date1 = datetime.datetime.strptime('2019-11-09', '%Y-%m-%d')
    date2 = datetime.datetime.strptime('2019-11-10', '%Y-%m-%d')
    sfu_yest = spark.sparkContext.parallelize([('SFU100',date1, 49.2771, -122.9146, 330.0, 12.0),('SFU100',date2, 49.2771, -122.9146, 330.0, 9.0)])
    sfu_tmax = spark.createDataFrame(sfu_yest, schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(sfu_tmax)

    predicted_value = predictions.select("prediction").first()[0]
    print('Predicted tmax tomorrow:', predicted_value)


if __name__ == '__main__':
    model_file = sys.argv[1]
    tmrw_model(model_file)