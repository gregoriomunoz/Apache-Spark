from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

people = []
people.append({'name':'Bob', 'age':45,'gender':'M'})
people.append({'name':'Gloria', 'age':43,'gender':'F'})
people.append({'name':'Albert', 'age':28,'gender':'M'})
people.append({'name':'Laura', 'age':33,'gender':'F'})
people.append({'name':'Simone', 'age':18,'gender':'T'})

rdd = sc.parallelize(people)

sumCount = rdd.aggregate(
    (0,0),
    (lambda x,y: (x[0] + y['age'], x[1] +1 )),
    (lambda x,y: (x[0] + y[0], x[1] + y[1] ))
)

print "Avg: ", sumCount[0]/float(sumCount[1])
