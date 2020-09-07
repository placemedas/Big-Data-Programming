import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    word_indexer = StringIndexer(inputCol="word", outputCol="word_idx", handleInvalid="error", stringOrderType="frequencyDesc")
    classifier = MultilayerPerceptronClassifier(layers=[3, 30, 11], featuresCol="features", labelCol="word_idx")
    # TODO: create a pipeline to predict RGB colours -> word

    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    rgb_prediction = rgb_model.transform(validation)

    # TODO: create an evaluator and score the validation data
    evaluator = MulticlassClassificationEvaluator(labelCol="word_idx", predictionCol="prediction", metricName="accuracy")
    score = evaluator.evaluate(rgb_prediction)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))

    # Logic for RGB to LAB starts here


    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=["word"])
    lab_sqltrans = SQLTransformer(statement = rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=["labL", "labA", "labB"], outputCol="features")
    lab_classifier = MultilayerPerceptronClassifier(layers=[3, 30, 11], featuresCol="features", labelCol="word_idx")
    lab_pipeline = Pipeline(stages=[lab_sqltrans, lab_assembler, word_indexer, lab_classifier])
    lab_model = lab_pipeline.fit(train)
    lab_prediction = lab_model.transform(validation)
    lab_score = evaluator.evaluate(lab_prediction)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model: %g' % (lab_score, ))

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)