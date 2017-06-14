from pyspark import SparkConf, SparkContext
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)

movieRDD = sc.textFile("textfiles/ratings-sample.csv")

def processLine(line):
    x= line.split(",")
    x= (x[1],[x[0],x[2]])
    return x

def processLine2(line):
    temp = line[1]
    values = []
    for x in temp:
        temp2 = (int(x[0]),float(x[1]))
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
        values.append(x)
    return (line[0], values)

def finalize(line):
    key = line[0]
    similars = line[1]
    similarDict={}
    for x in similars:
        for y in x:
            if similarDict.has_key(y):
                similarDict[y] += 1
            else:
                similarDict[y] = 1   
    sortedList = sorted(similarDict, key=similarDict.get, reverse=True)   
    if len(sortedList)>10:
        sortedList=sortedList[0:10]     
    return (key,sortedList)


userRDD = movieRDD.map(processLine).groupByKey().map(processLine2).filter(lambda x : len(x[1])>1).map(processUser).flatMap(lambda x: x).filter(lambda x : len(x[1])>0).groupByKey().map(processLine3).map(finalize).sortBy(lambda x:len(x[1]),False).collect()


print userRDD