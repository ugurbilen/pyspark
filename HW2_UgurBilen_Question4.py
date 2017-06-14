from pyspark import SparkConf, SparkContext
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)

movieRDD = sc.textFile("textfiles/ratings-sample.csv")

def processLine(line):
    x= line.split(",")
    x= (x[0],x[1],x[2])
    return x

def processLine2(line):
    temp = line[1]
    values = []
    for x in temp:
        temp2 = (x[0],x[1])
        values.append(temp2)
    return (line[0], values)
    
   
def processUser(line):
    ratings = line[1]
    matches = []
    count = len(ratings)
    for x in range(count):
        xList=[]
        for y in range(count):
            if((ratings[x][0]!=ratings[y][0]) and (ratings[x][1]==ratings[y][1])):
                xList.append(ratings[y][0]) 
        matches.append((ratings[x][0],xList))        
    return matches

def processLine3(line):
    values = []
    for x in line[1]:
        values.append(x[1])
    return (line[0],values)

def finalize(line):
    key = line[0]
    similars = line[1]
    similarDict={}
    for x in similars:
        if similarDict.has_key(x):
            similarDict[x] += 1
        else:
            similarDict[x] = 1   
    sortedList = sorted(similarDict, key=similarDict.get, reverse=True)   
    if len(sortedList)>10:
        sortedList=sortedList[0:10]     
    return (key,sortedList)

movieRDD = movieRDD.map(processLine)
userRDD = movieRDD.cartesian(movieRDD).filter(lambda x : x[0][0]!= x[1][0]).filter(lambda x: x[0][1]==x[1][1] and x[0][2]==x[1][2]).\
    map(lambda x: (x[0][0], x[1][0])).groupBy(lambda x:x[0]).map(processLine3).map(finalize)
    
userRDD.saveAsTextFile("textfiles/results11/")

#print userRDD