#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

depremRDD = sc.textFile("textfiles/depremler.txt")

def processLine(line):
    arr = line.split("\t")
    return float(arr[7])


print depremRDD.filter(lambda line:"1999" in line.split("\t")[1]).map(processLine).mean()

