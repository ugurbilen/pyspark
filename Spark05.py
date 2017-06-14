#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

textX = [("a",1),("a",2),("a",3),("b",1),("b",2),("c",11)]
textRDD = sc.parallelize(textX)

#depremRDD = sc.textFile("textfiles/depremler.txt")

def formatla(x):
    arr = []
    for y in x[1]:
        arr.append(y[1])        
    return (x[0],arr)

print textRDD.groupBy(lambda item:item[0]).map(formatla).sortBy(lambda item:item[0],False).collect()