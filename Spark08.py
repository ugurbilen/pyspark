#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

sayisalRDD = sc.textFile("sayisal")

def parseText(line):
    inn = line.index("rakamlar")+10
    nums = line[inn+2:inn+19]
    arr = nums.split("#")
    return [arr[0],arr[1],arr[2],arr[3],arr[4],arr[5]]

print sayisalRDD.coalesce(1).map(parseText).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1], False).collect()

