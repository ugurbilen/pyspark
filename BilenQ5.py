from pyspark import SparkConf, SparkContext
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
    appName("Python Spark SQL").getOrCreate()

worldRDD = sc.textFile("textfiles/world.txt")

def processLine(line):
    x= line.split(",")
    return (x[1],int(x[3]))

print worldRDD.map(processLine).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], False).collect()
