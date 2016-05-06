from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext("yarn-client", "s19433")
sqlContext = SQLContext(sc)

text_file = sc.textFile("/data/access_logs/access.log.2015-12-19")
#text_file = sc.textFile("/user/s19433/small.log")

def get_hit_hour(line):
	data = line.split(":")
	if len(data) < 2:
		return ("blank",1)
	hit_hour = line.split(":")[1]
	return (hit_hour, 1)


ip_count = text_file.map(lambda line: get_hit_hour(line)).reduceByKey(lambda a, b: a + b)
df = ip_count.toDF(["hour", "hit_count"])
df.sort(asc("hour")).show(25)
