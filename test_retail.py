import pytest
from lib.utils import get_pyspark_session
from lib.datareader import read_customer_data
from lib.ConfigReader import get_app_config
from lib.dataManupulation import findIndex,findIndexDF,findIndexDFCount


@pytest.mark.skip()
def test_count_customer(spark): 
    customer_count = read_customer_data("LOCAL",spark).count()
    assert customer_count == 100


@pytest.mark.skip()
def test_read_app():
    data = get_app_config("LOCAL")
    assert data["customers.file.path"] == "data/org.csv"
    assert data["customers.file.path"] == "data/org.csv"
    assert data["customers.file.path"] == "data/org.csv"

@pytest.mark.skip()
def testIndex1(spark,expected):
    data = read_customer_data("LOCAL",spark)
    print(expected)
    assert findIndexDF(data).collect() == expected.collect()

@pytest.mark.parametrize(
"num",
[(x) for x in range(1,5)]

)
def testIndex1(spark,num):
    data = read_customer_data("LOCAL",spark)
    assert findIndexDFCount(data,num) == 1
