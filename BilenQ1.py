from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

worldRDD = sc.textFile("textfiles/world.txt")


def processLine(line):
    x= line.split(",")
    return int(x[3])

print worldRDD.map(processLine).sum()

