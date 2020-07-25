import pytest

from pyspark.ml.fpm import FPGrowth

spark = pytest.spark

def test_association_rules():
    data = spark.createDataFrame(
                [([1, 2], ), ([1, 2], ), ([1, 2, 3], ), ([1, 3], )],
                ["items"])

    fp = FPGrowth()
    fpm = fp.fit(data)

    expected_association_rules = spark.createDataFrame(
        [([3], [1], 1.0, 1.0), ([2], [1], 1.0, 1.0)],
        ["antecedent", "consequent", "confidence", "lift"]
    )

    actual_association_rules = fpm.associationRules

    assert actual_association_rules.subtract(expected_association_rules).count() == 0
    assert expected_association_rules.subtract(actual_association_rules).count() == 0

def test_freq_itemsets():
    data = spark.createDataFrame(
                [([1, 2], ), ([1, 2], ), ([1, 2, 3], ), ([1, 3], )],
                ["items"])

    fp = FPGrowth()
    fpm = fp.fit(data)

    expected_freq_itemsets = spark.createDataFrame(
        [([1], 4), ([2], 3), ([2, 1], 3), ([3], 2), ([3, 1], 2)],
        ["items", "freq"]
    )
    actual_freq_itemsets = fpm.freqItemsets

    assert actual_freq_itemsets.subtract(expected_freq_itemsets).count() == 0
    assert expected_freq_itemsets.subtract(actual_freq_itemsets).count() == 0
