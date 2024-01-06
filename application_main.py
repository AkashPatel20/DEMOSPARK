import sys
from lib.utils import get_pyspark_session
from lib.datareader import read_customer_data
from lib.dataManupulation import findIndex
from lib.logger import *


if __name__ == '__main__':
    print("entered")
    print(len(sys.argv))
    if len(sys.argv) < 2:
        print("bhai dhang se dal")
        sys.exit(-2)
    env = sys.argv[1]
    spark = get_pyspark_session(env)
    logger = Log4J(spark)
    logger.info("hum aa gye")
    data = read_customer_data(env,spark)
    final_data = findIndex(data)
    print(final_data[0][1])
    print("end of script")