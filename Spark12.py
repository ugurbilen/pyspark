#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

spark = SparkSession.builder.appName("Python Spark SQL").getOrCreate()

depremRDD = sc.textFile("textfiles/depremler.txt").map(lambda x:x.split("\t"))
df=spark.createDataFrame(depremRDD)

df.select("_1").show()
#df.show()