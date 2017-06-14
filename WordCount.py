#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
import time
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

start = time.time()
fileRDD = sc.textFile("hdfs://10.36.97.5/user/ugurbilen/depremler-1900-2015-3.5.txt")

print fileRDD.map(lambda line:line.split(" ")).\
flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y : x+y).collect()

fileRDD.saveAsTextFile("wordcount")
end =time.time()

print (end-start + "--> bitti")
