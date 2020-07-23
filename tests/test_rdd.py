import pytest

sc = pytest.sc

def test_range():
    assert sc.range(1,1).count() == 0
    assert sc.range(1, 0, -1).count() == 1
    assert sc.range(0, 1 << 40, 1 << 39).count() == 2

def test_id():
    rdd = sc.parallelize(range(10))
    id = rdd.id()

    assert id == rdd.id()

    # doesn't work
    # rdd2 = rdd.map(str).filter(bool)
    # id2 = rdd2.id()
    #
    # assert id + 1 == id2
    # assert id2 == rdd2.id()

def test_empty_rdd():
    rdd = sc.emptyRDD()
    assert rdd.isEmpty() == True

def test_sum():
    assert sc.emptyRDD().sum() == 0
    assert sc.parallelize([1, 2, 3]).sum() == 6

def test_flatmap():
    rdd = sc.parallelize([2, 3, 4])
    assert sorted(rdd.flatMap(lambda x: range(1, x)).collect()) == [1, 1, 1, 2, 2, 3]

# doesn't work
# def test_to_localiterator():
#     rdd = sc.parallelize([1, 2, 3])
#     it = rdd.toLocalIterator()
#     assert [1, 2, 3] == sorted(it)
