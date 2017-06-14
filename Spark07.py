#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

depremRDD = sc.textFile("textfiles/depremler.txt")

def processLine(line):
    arr=line.split("\t")
    return (arr[4],arr[5],arr[7],arr[14])

def region(deprem):
    enlem =float(deprem[0])
    boylam = float(deprem[1])
    
    if enlem<31 and boylam>39:
        return "Region 1"
    elif enlem>31 and enlem<36 and boylam>39:
        return "Region 2"
    elif enlem>36 and enlem<41 and boylam>39:
        return "Region 3"
    elif enlem>41 and boylam>39:
        return "Region 4"
    elif enlem<31 and boylam<39:
        return "Region 5"
    elif enlem>31 and enlem<36 and boylam<39:
        return "Region 6"
    elif enlem>36 and enlem<41 and boylam<39:
        return "Region 7"
    elif enlem>41 and boylam<39:
        return "Region 8"
    else:
        return "Region 9"

        
def formatla(x):
    toplam = 0
    depremSayisi = len(x[1])
    
    for y in x[1]:
        toplam+=float(y[2])   
        
    ortalama = toplam/depremSayisi
    return (x[0],depremSayisi,ortalama)

print depremRDD.map(processLine).groupBy(region).map(formatla).sortBy(lambda x:x[2],False).collect()