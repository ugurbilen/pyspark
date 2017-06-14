#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)


ratingsRDD = sc.textFile("hdfs://10.36.97.5/user/ahmetdemirelli/ratings.csv")#("textfiles/ratings-sample.csv")

def processLine(line):
    arr = line.split(",")
    return (arr[1],arr[2])

def movieRatingOrtalama(item):

    sum = 0
    for x in item[1]:
        sum += float(x[1])
        
    return (item[0],sum/len(item[1]),len(item[1]))

print ratingsRDD.map(processLine).groupBy(lambda x:x[0]).map(movieRatingOrtalama).sortBy(lambda x:(x[1],x[2]),False).take(10)

    
    
    
    
    
    
    
    
    




