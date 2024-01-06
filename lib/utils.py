from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config

def get_pyspark_session(env):
    if env == "LOCAL":
        return SparkSession.builder. \
            config(conf = get_pyspark_config(env)). \
            config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties"). \
            getOrCreate()