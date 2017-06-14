from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
    appName("Python Spark SQL").getOrCreate()


'''
3. Using Sayisal Loto dataset, find top 10 most commonly drawn number pairs.
'''


sayisalRDD = sc.textFile("sayisal")

def parseAndPair(line):
    inn = line.index("rakamlar")+10
    nums = line[inn+2:inn+19]
    arr = nums.split("#")
    pairs = []
    
    #TO AVOID DUPLICATE PAIRS, ADD PAIRS TO THE LIST SORTED
    for x in range(len(arr)):
        for y in range(x+1,len(arr)):
            if arr[x]>arr[y]:
                pairs.append((arr[y],arr[x]))
            else:
                pairs.append((arr[x],arr[y]))
    
    return pairs

pairRDD = sayisalRDD.flatMap(parseAndPair).map(lambda x:Row(pair = x))      #DETERMINE PAIRS AND CONVERT TO ROW
df = spark.createDataFrame(pairRDD);                                        #CREATE DATAFRAME
df.createOrReplaceTempView("pairs")
resultSet = spark.sql("select pair,count(*) as counts from pairs group by pair order by counts desc")       #EXECUTE COMMAND TO GET RESULT

resultSet.show(10)
