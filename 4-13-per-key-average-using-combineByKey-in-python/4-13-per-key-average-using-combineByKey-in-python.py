from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

nums = sc.parallelize(
        [("coffee", 1), ("pandas", 2), ("coffee", 3), ("very", 4)])

#combineByKey.(createCombiner, mergeValue, mergeCombiners)

sumCount = nums.combineByKey(
    (lambda x: (x, 1)),
    (lambda x,y: (x[0] + y, x[1] +1 )),
    (lambda x,y: (x[0] + y[0], x[1] + y[1] ))
)

r = sumCount.map(lambda keyvals: (keyvals[0], keyvals[1][0]/keyvals[1][1])).collectAsMap()
print r
