from pyspark import SparkContext, SparkConf

sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

numbers=range(100)
print numbers

numbersRDD = sc.parallelize(numbers)


def square(sayi):
    return sayi*sayi

def ifilter(sayi):
    if((sayi%2)==0):
        return True
    return False

filteredRDD = numbersRDD.filter(ifilter)

yeniRDD = filteredRDD.map(square)

print yeniRDD.collect()