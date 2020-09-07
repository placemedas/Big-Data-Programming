import sys
import numpy as np
import matplotlib as plt
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

def weather_schema():
    tmaxschema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])
    return tmaxschema

def main(input,model_file):
    tmax_schema = weather_schema()
    temp = spark.read.csv(input, schema=tmax_schema)
    train, validation = temp.randomSplit([0.75, 0.25],seed = 70)
    train = train.cache()
    validation = validation.cache()
    train.registerTempTable("train")
    # Query with out using yesterday max
    #query = "SELECT latitude,longitude,elevation,DAYOFYEAR(date) as day,tmax as tmax FROM __THIS__"

    # Query with using yesterday max
    query = "SELECT td.latitude, td.longitude, td.elevation, DAYOFYEAR(td.date) as day, td.tmax as tmax, yd.tmax as yd_tmax FROM __THIS__ as td INNER JOIN __THIS__ as yd ON date_sub(td.date, 1) = yd.date AND td.station = yd.station"
    temp_sqltrans = SQLTransformer(statement= query)

    # Vector Assembler with out using yesterday max
    #temp_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "day"],outputCol="features")

    # Vector Assembler using yesterday max
    temp_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "day", "yd_tmax"], outputCol="features")

    # Fitting different Models
    #regressor = DecisionTreeRegressor(featuresCol="features",labelCol='tmax',predictionCol='prediction')
    #regressor = RandomForestRegressor(featuresCol="features", labelCol='tmax', predictionCol='prediction')
    regressor = GBTRegressor(featuresCol="features", labelCol='tmax', predictionCol='prediction')
    #regressor = GeneralizedLinearRegression(family="gaussian",link = "identity", featuresCol="features", labelCol='tmax', predictionCol='prediction')

    evaluator = RegressionEvaluator(labelCol="tmax", predictionCol="prediction", metricName="rmse")

    temp_pipeline = Pipeline(stages=[temp_sqltrans, temp_assembler, regressor])
    tmax_model = temp_pipeline.fit(train)
    tmax_prediction = tmax_model.transform(validation)


    rmse_score = evaluator.evaluate(tmax_prediction)

    evaluator_r2 = RegressionEvaluator(labelCol="tmax", predictionCol="prediction", metricName="r2")
    r2_score = evaluator_r2.evaluate(tmax_prediction)

    print('Validation  RMSE score for TMAX model: %g' % (rmse_score,))
    print('Validation  R2 score for TMAX model: %g' % (r2_score,))


    tmax_model.write().overwrite().save(model_file)

if __name__ == '__main__':
    input = sys.argv[1]
    model_file = sys.argv[2]
    main(input, model_file)


