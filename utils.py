import configparser
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def get_dataframe(spark: SparkSession, filepath: str):
    df = spark.read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv(filepath)
    return df