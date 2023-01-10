from pyspark.sql import *
from collections import namedtuple
from utils import get_spark_app_config,get_dataframe
rddRecord = namedtuple("rddRecord", ["DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count"])
if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    print("Starting the session")
    sparkContext = spark.sparkContext
    rdd = sparkContext.textFile("C:\Kaminee\KamineeWork\pythonprojects\YogeshSpark\data\summary_rdd.csv")
    colsRDD = rdd.map(lambda line: line.split(","))
    selectRDD = colsRDD.map(lambda cols: rddRecord(cols[0], cols[1], int(cols[2])))
    filteredRDD = selectRDD.filter(lambda r: r.count > 1)
    kvRDD = filteredRDD.map(lambda r: (r.ORIGIN_COUNTRY_NAME, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1+v2)
    colsList = colsRDD.collect()
    for x in colsList:
        print(x)



    print("ending the session")



