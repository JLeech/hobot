from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from itertools import combinations


sc = SparkContext("yarn-client", "s19433")
sqlContext = SQLContext(sc)

text_file = sc.textFile("/data/social_graph/part-v008")
#text_file = sc.textFile("/user/s19433/small.log")

def get_pairs_of_connections(line):
	line_data = line.split("\t")

	friends_raw_data = line_data[1].split("),(")
	friends_raw_data[0] = friends_raw_data[0][2:]
	friends_raw_data[-1] = friends_raw_data[-1][:-2]
	friends_ids = [ int( val.split(",")[0] ) for val in friends_raw_data ]
	result = []

	for combination in combinations(friends_ids, 2):
		result.append(str(combination)[0] + " " + str(combination)[1])
			

	for friend_id in friends_ids :
		if main_id > friend_id :
			result.append(str(main_id) + " " + str(friend_id))
		else :
			result.append(str(friend_id) + " " + str(main_id))
	return result

ids_count = text_file.flatMap(lambda line: get_pairs_of_connections(line)).map(lambda ids: (ids, 1)).reduceByKey(lambda a, b: a + b)
df = ids_count.toDF(["idents", "count"]).sort(desc("count"))
df.show(10)
df.write.save("/user/s19433/spark_lab5_result")
