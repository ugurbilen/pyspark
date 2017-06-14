from pyspark import SparkConf, SparkContext
from operator import itemgetter

sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)

#Read files
ratingsRDD = sc.textFile("textfiles/ratings-sample.csv")
movieRDD = sc.textFile("textfiles/movies.csv")
tagsRDD = sc.textFile("textfiles/tags.csv")

#Get movieID and Rating Columns from file and return a tuple
def processRatings(rating):
    rating= rating.split(",")
    return (rating[1],rating[2])

#calculate average rating for a movie
def calculateAverage(line):
    sum=0.0
    ratingList = []
    
    for x in line[1]:
        ratingList.append(x)
      
    for rating in ratingList:
        sum += float(rating)
        
    avgRate = (sum/len(ratingList))
    
    return (line[0],avgRate)

def ungroup(line):
    ratingList = []
    
    for x in line[1]:
        ratingList.append((x[0],x[1][1]))
    return (line[0], ratingList)   

def getMax(line):
    ratingList = line[1]
    maxRating = max(ratingList,key=itemgetter(1))
    return line[0], maxRating


ratingsRDD = ratingsRDD.map(processRatings)

#calculate Average rating per movie
avgRateRDD =  ratingsRDD.groupByKey().map(calculateAverage)

#Process Tags
def processTags(tag):
    tags= tag.split(",")
    p_tags = (tags[1],tags[2])
    return p_tags

tagsRDD = tagsRDD.map(processTags)

joinedRDD = tagsRDD.join(avgRateRDD)

#joinedRDD.distinct().groupBy(lambda x : x[1][0]).map(ungroup).map(getMax).map(lambda x : (x[0],int(x[1][0]),x[1][1])).saveAsTextFile("textfiles/results/")
print joinedRDD.distinct().groupBy(lambda x : x[1][0]).map(ungroup).map(getMax).map(lambda x : (x[0],int(x[1][0]),x[1][1])).collect()


