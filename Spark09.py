#spark konfigurasyonunu ayaga kaldirmak ve spark contextini yuklemek
from pyspark import SparkContext, SparkConf
sparkConf = SparkConf().setMaster("local[*]").setAppName("Ugur Spark 1")
sc= SparkContext(conf=sparkConf)

#rdd1 = sc.parallelize(range(1,10))

#map
#print rdd1.map(lambda x:(x,x*x,x*x*x)).collect()

#filter
#print rdd1.filter(lambda x:x>5).collect()

#rdd2 = sc.parallelize([[1,2,3], [4,5,6],[7,8,9]])
#print rdd2.flatMap(lambda x:x+x).collect() 

#rdd3 = sc.parallelize(["ahmet demirelli ege ozan ilkay", "ali desidero", "ugur bilen"])
#print rdd3.flatMap(lambda x:x.split(" ")).collect() #flatMap yerine map kullansaydik tek array degil her bir arrayi split edilmis sekilde donecekti.

#print rdd1.sample(False,0.9).collect()

#rdd1=sc.parallelize([("a",3),("a",1),("a",2),("a",3),("a",4),])
#rdd2=sc.parallelize([("b",1),("a",2),("a",3),("a",4),])

#print rdd1.union(rdd2).collect()

#rdd1=sc.parallelize([("a",3),("a",1),("a",2),("a",3),("a",4),])
#rdd2=sc.parallelize([("b",1),("b",2),("b",3),("b",4),("a",4)])

#print rdd1.intersection(rdd2).collect()

#rdd1=sc.parallelize([("a",3),("a",1),("a",2),("a",3),("a",4)])
#print rdd1.distinct().collect()

'''
rdd1=sc.parallelize([("a",3),("a",1),("a",2),("a",3),("a",4),("c",44)])
rdd2=sc.parallelize([("b",1),("b",2),("b",3),("b",4),("a",4)])

def formatGroup(x):
    gr = []
    
    for a in x[1]:
        gr.append(a)
        
    return (x[0],gr)


print rdd1.groupByKey().map(formatGroup).collect()
'''
'''
rdd1=sc.parallelize([("a",3),("a",1),("c",44)])
print rdd1.reduceByKey(lambda x,y : x+y).collect()
'''

'''
rdd1=sc.parallelize([("a",3),("a",1),("a",2),("a",3),("a",4),("c",44)])
print rdd1.aggregateByKey(1,lambda x,y:x*y,lambda x,y:x+y).collect()
'''

'''
rdd1=sc.parallelize([("a",3),("a",1),("a",2),("a",3),("a",4),("c",44)])
print rdd1.map(lambda x: (x[1],x[0])).sortByKey().collect()
'''

'''
rdd1=sc.parallelize([("a",1),("a",2),("a",3),("a",4),("a",5),("c",1)])
rdd2=sc.parallelize([("a",33),("a",66),("a",99),("a",22)])
print rdd1.join(rdd2).collect()
'''

'''
rdd1=sc.parallelize([("a",1),("a",2),("b",1)])
rdd2=sc.parallelize([("a",33),("a",66),("b",22)])

def grupla(item):
    gr1=[]
    gr2=[]
    
    for x in item[1]:
        gr1.append(x)
 
    for x in item[2]:
        gr2.append(x)       
        
    return (item[0],gr1,gr2)
print rdd1.cogroup(rdd2).map(grupla).collect()
'''






