from pyspark.sql import *
from utils import get_spark_app_config,get_dataframe

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    print("Starting the session")
    df = get_dataframe(spark, "C:\Kaminee\KamineeWork\pythonprojects\YogeshSpark\data\summary.csv")
    df.registerTempTable("details_tbl")
    qDF = spark.sql("select ORIGIN_COUNTRY_NAME,sum(count) as count_sum from details_tbl group by ORIGIN_COUNTRY_NAME order by ORIGIN_COUNTRY_NAME desc")
    qDF.show()
    # df.show()
    print("ending the session")



