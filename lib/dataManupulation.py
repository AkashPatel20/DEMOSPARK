from pyspark.sql.functions import *

def findIndex(org_df):
    return org_df.select("Name","Country").filter("index = 1").collect()

def findIndexDF(org_df):
    return org_df.filter("index = 1")


def findIndexDFCount(org_df,num):
    return org_df.filter(org_df["index"] == num).count()