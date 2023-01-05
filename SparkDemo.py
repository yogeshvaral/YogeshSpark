from pyspark import SparkConf, SparkContext
sc = SparkContext(master="local", appName="SparkDemo")
print(sc.textFile("mydata.txt").first())
