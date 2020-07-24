import pytest

from pyspark.sql.types import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import NaiveBayes

spark = pytest.spark
sc = pytest.sc

def test_naive_bayes():
    df = spark.createDataFrame([
        Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
        Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
        Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0]))])

    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", weightCol="weight")
    model = nb.fit(df)
    model.setFeaturesCol("features")

    test0 = sc.parallelize([Row(features=Vectors.dense([1.0, 0.0]))]).toDF()
    features = test0.head().features

    result = model.predict(features)

    assert result == 1.0
