import pytest
from lib.utils import get_pyspark_session

@pytest.fixture
def spark():
   data = get_pyspark_session("LOCAL")
   yield data
   data.stop()

@pytest.fixture
def expected(spark):
   data = spark.read.format("csv"). \
    option("header",False). \
    option("inferSchema",True). \
    load("data/test_result.csv")
   return data

