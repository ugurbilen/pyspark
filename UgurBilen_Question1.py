from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row

sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
    appName("Python Spark SQL").getOrCreate()

'''
#===============================================================================
# 1. Using earthquake dataset find which season of each year do most earthquakes occurred in years between 1995 and 2017?
#===============================================================================
'''

# DETERMINE SEASON FOR EACH LINE OF DATA
def addSeason(line):
    month = int(line[2][5:7])
    
    if (month == 12 or month<=2):
        line.append("Kis")
    elif (month>=3 and month<=5):
        line.append("Ilkbahar")
    elif (month >=6 and month <=8):
        line.append("Yaz")   
    else:
        line.append("Sonbahar")
        
    return line                                                                                                                                                                                                                                                                                                                                                                   
    
    
depremRDD = sc.textFile("textfiles/depremler.txt")

formattedRDD = depremRDD.map(lambda line : line.split("\t")).map(addSeason)     # ADD CORRESPONDING SEASON
seasonRDD = formattedRDD.map(lambda x :Row(DepremId=x[0],Tarih = x[2],Yil=x[2][0:4], Mevsim=x[15],Buyukluk=float(x[7]))) #CONVERT TO ROWS WITH HEADERS

df = spark.createDataFrame(seasonRDD);          #CREATE DATAFRAME
df.createOrReplaceTempView("deprem")

#QUERY THE NUMBER OF EARTHQUAKES IN EACH SEASON OF EACH YEAR >1994
resultSet1 = spark.sql("select Yil,Mevsim,count(DepremId) as Sayi from deprem where Yil>1994 group by Yil,Mevsim order by Yil,Sayi desc")   
resultSet1.createOrReplaceTempView("seasons")

#GET A LIST OF YEARS IN SCOPE
resultSet2 = spark.sql("select distinct Yil from deprem where Yil>1994 order by Yil desc")      
years = resultSet2.rdd.flatMap(lambda x: x).collect()

#FOR EACH YEAR DETERMINE AND PRINT THE SEASON WITH MAXIMUM EARTHQUAKES
for year in years:
    command = ("select * from seasons where Yil = "+year+" and Sayi = (select max(Sayi) from seasons where Yil = "+ year +" group by Yil)")
    #print command
    resultSet4=spark.sql(command)
    resultSet4.show()


