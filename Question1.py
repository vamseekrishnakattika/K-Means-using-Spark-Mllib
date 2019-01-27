from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors


conf = SparkConf().setMaster("local").setAppName("Question1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

item_user_mat = sc.textFile("itemusermat").map(lambda x: x.split(" ")).map(lambda x: [int(y) for y in x[1:]])
item_user = sc.textFile("itemusermat").map(lambda x: x.split(" ")).map(lambda x: [int(y) for y in x])

clusters = KMeans.train(item_user_mat, 10, maxIterations=100, initializationMode="kmeans||")
item_user = item_user.map(lambda x: (x[0],clusters.predict(x[1:])))

movie = sc.textFile('movies.dat').map(lambda x: x.split("::")).map(lambda line: (int(line[0]),(line[1],line[2])))
tmp = movie.join(item_user)

final = tmp.map(lambda x: (x[1][1],(x[0],x[1][0][0],x[1][0][1],x[1][1]))).groupByKey().sortByKey(ascending=True).map(lambda x: list(x[1])[0:5]).flatMap(lambda x: x)
#final.foreach(print)
final.repartition(1).saveAsTextFile("Question1")
