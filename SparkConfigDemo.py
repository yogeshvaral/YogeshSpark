from pyspark.sql import *
from utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    print("Starting the session")
    conf_out = spark.sparkContext.getConf()
    print(conf_out.toDebugString())
    print("ending the session")