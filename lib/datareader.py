from lib import ConfigReader


def read_customer_data(env,spark_obj):
    data = ConfigReader.get_app_config(env)
    path = data["customers.file.path"]
    return spark_obj.read. \
    format("csv"). \
    option("header", True). \
    option("inferSchema", True). \
    load(path)
 

