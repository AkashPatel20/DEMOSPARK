from pyspark.sql import SparkSession
import logging


class Log4J:
    def __init__(self, spark_session):

        log4j = spark_session._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("retail-LOCAL")

    def error(self,message):
        self.logger.error(message)

    def info(self,message):
        self.logger.info(message)

