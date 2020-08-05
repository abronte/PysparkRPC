import pytest

sc = pytest.sc

def test_broadcast_variable():
    b = sc.broadcast([1, 2, 3, 4, 5])
    print(b)
    assert b.value == [1, 2, 3, 4, 5]

    rows = sc.parallelize([0, 0]).flatMap(lambda x: [1,2,3,4,5]).collect()

    assert rows == [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
