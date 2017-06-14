#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from sklearn.svm import SVC
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

depremRDD = sc.textFile("textfiles/depremler.txt")

def processLine(line):
    arr=line.split("\t")
    return (arr[2],arr[3],arr[7],arr[14])

def geceGunduz(deprem):
    saat=float(deprem[1][0:2])
    if saat<20 and saat>7:
        return "Gunduz"
    return "Gece"
        
def formatla(x):
    arr = []
    for y in x[1]:
        arr.append(y)        
    return (x[0],arr)

print depremRDD.map(processLine).groupBy(geceGunduz).map(formatla).take(3)