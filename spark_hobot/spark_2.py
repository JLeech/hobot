from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext("yarn-client", "s19433")
sqlContext = SQLContext(sc)

text_file = sc.textFile("/data/access_logs/access.log.2015-12-19")
#text_file = sc.textFile("/user/s19433/small.log")

def get_ip_and_data_size(line):
	line_data = line.split("\"")
	ip = line_data[0].split(" ")[0]
	data_size = int(line_data[2].split(" ")[1])
	return (ip, data_size)


ip_count = text_file.map(lambda line: get_ip_and_data_size(line)).reduceByKey(lambda a, b: a + b)
df = ip_count.toDF(["ip", "value"]).sort(desc("value"))
df.show()
df.write.save("/user/s19433/spark_lab2_result")

