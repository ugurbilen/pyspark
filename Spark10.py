#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)


'''
depremRDD = sc.textFile("textfiles/depremler.txt")

def processLine(line):
    arr = line.split()
    return (arr[4],arr[5],arr[7],arr[14])


depremRDD.map(processLine).saveAsTextFile("textfiles/results")
print "finito"
'''

'''
rdd = sc.parallelize([1,3,4,56,7,8,9,99,45,332,23,233])
print rdd.stats() #tum bilgileri yazdirir
print rdd.mean()
print rdd.max()
print rdd.min()
print rdd.stdev()
print rdd.count()
'''
'''
rdd = sc.parallelize([1,3,4,56,7,8,9,99,45,332,23,233])
print rdd.take(12)
print rdd.takeOrdered(12)# karsilastirilabilir olmali, tekli olmali tuple degil
'''

'''
newSample = rdd.takeSample(False,5)
print sc.parallelize(newSample) #parallelize ederek action method outputunu tekrar transformation icin kullanabiliriz
'''

rdd = sc.parallelize([("Ahmet",1),("Ali",2),("Veli",3)])

def collectNames(itemX, itemY):
    return (itemX[0]+" - " + itemY[0], itemX[1]+itemY[1])

print rdd.reduce(collectNames)


    
    
    
    
    
    
    
    
    
    
    




