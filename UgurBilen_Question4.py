from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row
from pyspark.sql.functions import monotonically_increasing_id
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
    appName("Python Spark SQL").getOrCreate()

'''
4. Using DollarDataset find top 5 greatest daily changes (both positive and negative) in historical dollar data. (You may need line numbers to make things easier)
'''
def processLine(line):
    arr= line.split(";")
    tarih =arr[0].strip()
    #id = tarih[6:11]+tarih[3:5]+tarih[0:2]
    kur = float(arr[1].strip())
                
    if kur>10:
        kur = kur/1000000   
        
    return [tarih,kur]

def rowToList(row):
    return [row[2],row[0],row[1]]
  
dollarRDD = sc.textFile("textfiles/DollarDataset.txt")
formattedRDD = dollarRDD.map(processLine).map(lambda x: Row(Date = x[0], Price = x[1])).filter(lambda x: x[1]>0)
df = spark.createDataFrame(formattedRDD);
df2 = df.withColumn("id", monotonically_increasing_id())

table = df2.rdd.map(rowToList).collect()


table[0].append(float(0.0))
table[0].append(float(0.0))
for x in range(1, len(table)):
    today = float(table[x][2])
    previous = float(table[x-1][2])
    diff = abs(today-previous)
    rate = diff/previous
    table[x].append(diff)
    table[x].append(rate)
    
finalRDD = sc.parallelize(table)
df3 = spark.createDataFrame(finalRDD,["id","date","value","difference","change_rate"])

df3.createOrReplaceTempView("dollar")
spark.sql("select * from dollar order by difference desc").show(5)

spark.sql("select * from dollar order by change_rate desc").show(5)



