#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

numbers=range(100)
numRDD=sc.parallelize(numbers)

def ciftSayilar(x):
    if(x%2==0):
        return True
    else:
        return False
    

numRDD.filter(ciftSayilar)

filteredRDD=numRDD.filter(lambda x:(x%2==0)) #transformed data

print filteredRDD

