from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
    appName("Python Spark SQL").getOrCreate()

worldRDD = sc.textFile("textfiles/world.txt")

def processLine(line):
    x= line.split(",")
    x.append(len(x[0]))
    return x

worldRDD = worldRDD.map(processLine).map(lambda x : Row(Country=x[0],Continent=x[1],Capital=x[2],Population=int(x[3]),Name_Length = x[4]))

df = spark.createDataFrame(worldRDD);

df.createOrReplaceTempView("world")
spark.sql("select Country,Continent,Capital,Population,Name_Length from world order by Name_Length desc").show(1)


