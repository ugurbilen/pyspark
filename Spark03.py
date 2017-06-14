#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

fileRDD = sc.textFile("textfiles/text.txt")

print fileRDD.map(lambda line:line.split(" ")).\
flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y : x+y).collect()