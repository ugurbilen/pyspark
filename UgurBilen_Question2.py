from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row
from math import cos, asin, sqrt
from datetime import datetime

sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)

from pyspark.sql import SparkSession
spark = SparkSession.builder.\
    appName("Python Spark SQL").getOrCreate()


'''
2. Using earthquake dataset find the list of foreshocks and aftershocks (within 20 km and in 24 hours) for the earthquakes that magnitude is greater than 4.0.
'''
foreshocks = {}
aftershocks = {}


#Haversine Formula Distance implementation
def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a))



def strToDatetime(tarih):
    #insert each earthquake into dictionary with DepremID as key and other columns as list
    tarihsaat = tarih
    tarih_saat = datetime.strptime(tarihsaat, '%Y.%m.%d %H:%M:%S.%f')
    return tarih_saat

def typeCastDates(dateList):
    index=0
    length = len(dateList)
    while index<length:
        swap=strToDatetime(dateList[index])
        dateList[index] =swap
        index = index+1

def typeCastEnlem(enlemList):
    index=0
    length = len(enlemList)
    while index<length:
        swap=float(enlemList[index])
        enlemList[index] =swap
        index = index+1   
        
def typeCastBoylam(boylamList):
    index=0
    length = len(boylamList)
    while index<length:
        swap=float(boylamList[index])
        boylamList[index] =swap
        index = index+1     
        
#IN THE LIST, MOVE TO NEIGHBOUR (EARLIER) QUAKE WHILE THAT HAPPENED WITHIN 24 HOURS AND IF IT IS WITHIN 20KMS ADD TO THE AFTERSHOCKS    
def checkForeshocks(p_index,p_length,foredict):
    temp=p_index+1
    foreList = []
    while temp<p_length:
        timediff= tarihler[p_index]-tarihler[temp]                
        if timediff.days==0:
            p_distance = distance(enlemler[p_index],enlemler[temp],boylamlar[p_index],boylamlar[temp])
            if p_distance<=20:
                foreList.append(depremIdler[temp])
            temp=temp+1
        else:
            break   
    foredict[depremIdler[p_index]]=foreList

#IN THE LIST, MOVE TO NEIGHBOUR (LATER) QUAKE WHILE THAT HAPPENED WITHIN 24 HOURS AND IF IT IS WITHIN 20KMS ADD TO THE AFTERSHOCKS    
def checkAftershocks(p_index,p_length,afterdict):
    temp=p_index-1
    afterList = []
    try:
        while temp>=0:
            timediff= tarihler[p_index]-tarihler[temp]               
            if timediff.days == -1:
                p_distance = distance(enlemler[p_index],enlemler[temp],boylamlar[p_index],boylamlar[temp])
                if p_distance<=20:
                    afterList.append(depremIdler[temp])
                temp=temp-1   
            else:
                break
        afterdict[depremIdler[p_index]]=afterList
    except:
        print "error"
        
        
def strToInt(depremId):
    x = int(depremId)
    return x

depremRDD = sc.textFile("textfiles/depremler.txt")

formattedRDD = depremRDD.map(lambda line : line.split("\t")).map(lambda x :Row(DepremId=x[0],Tarih=x[2]+ " " + x[3],Enlem=x[4],Boylam=x[5],Buyukluk=float(x[7])))
filteredRDD = formattedRDD.filter(lambda x:x[1]>4.0)        #FILTER EARTHQUAKES GREATER THAN 4.0


df = spark.createDataFrame(formattedRDD)
df.createOrReplaceTempView("tumdepremler")

df2 = spark.createDataFrame(filteredRDD)
df2.createOrReplaceTempView("buyukdepremler")


################3    GET VALUES FOR ALL EARTHQUAKES TO LISTS ################
#get DepremId values to a list
resultSet1 =  spark.sql("select DepremId from tumdepremler")
depremIdler = resultSet1.rdd.flatMap(lambda x: x).collect()

#get Boylam values to a list
resultSet2 =  spark.sql("select Boylam from tumdepremler")
boylamlar = resultSet2.rdd.flatMap(lambda x: x).collect()
typeCastBoylam(boylamlar)

#get Enlem values to a list
resultSet3 =  spark.sql("select Enlem from tumdepremler")
enlemler = resultSet3.rdd.flatMap(lambda x: x).collect()
typeCastEnlem(enlemler)

#get Buyukluk values to a list
resultSet4 =  spark.sql("select Buyukluk from tumdepremler")
buyuklukler = resultSet4.rdd.flatMap(lambda x: x).collect()

#get Tarih values to a list
resultSet5 =  spark.sql("select Tarih from tumdepremler")
tarihler = resultSet5.rdd.flatMap(lambda x: x).collect()
typeCastDates(tarihler)

############################    GET VALUES FOR STRONG(>4.0) EARTHQUAKES TO LISTS ######################
#get DepremId values to a list
resultSet6 =  spark.sql("select DepremId from buyukdepremler")
f_depremIdler = resultSet6.rdd.flatMap(lambda x: x).collect()

#get Boylam values to a list
resultSet7 =  spark.sql("select Boylam from buyukdepremler")
f_boylamlar = resultSet7.rdd.flatMap(lambda x: x).collect()
typeCastBoylam(f_boylamlar)

#get Enlem values to a list
resultSet8 =  spark.sql("select Enlem from buyukdepremler")
f_enlemler = resultSet8.rdd.flatMap(lambda x: x).collect()
typeCastEnlem(f_enlemler)

#get Buyukluk values to a list
resultSet9 =  spark.sql("select Buyukluk from buyukdepremler")
f_buyuklukler = resultSet9.rdd.flatMap(lambda x: x).collect()

#get Tarih values to a list
resultSet10 =  spark.sql("select Tarih from buyukdepremler")
f_tarihler = resultSet10.rdd.flatMap(lambda x: x).collect()
typeCastDates(f_tarihler)

## FOR EACH BIG EARTHQUAKE (>4.0) LOOK FOR FORESHOCKS AND AFTERSHOCKS

for deprem in f_depremIdler:
    realIndex = strToInt(deprem) - 1    
    checkForeshocks(realIndex, len(depremIdler), foreshocks)    
    checkAftershocks(realIndex, len(depremIdler), aftershocks)
    
    

print "printing foreshocks"    
for k, v in foreshocks.items():
    if len(v)>0:
        print(k, v)

print "printing aftershocks"    
for k, v in aftershocks.items():
    if len(v)>0:
        print(k, v)



