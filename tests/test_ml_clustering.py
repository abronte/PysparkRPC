import pytest

from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans

spark = pytest.spark

def test_kmeans_cosine_distance():
    data = [(Vectors.dense([1.0, 1.0]),), (Vectors.dense([10.0, 10.0]),),
            (Vectors.dense([1.0, 0.5]),), (Vectors.dense([10.0, 4.4]),),
            (Vectors.dense([-1.0, 1.0]),), (Vectors.dense([-100.0, 90.0]),)]
    df = spark.createDataFrame(data, ["features"])

    kmeans = KMeans(k=3, seed=1, distanceMeasure="cosine")
    model = kmeans.fit(df)
    result = model.transform(df).collect()

    assert result[0].prediction == result[1].prediction
    assert result[2].prediction == result[3].prediction
    assert result[4].prediction == result[5].prediction

