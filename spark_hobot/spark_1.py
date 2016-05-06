from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext("yarn-client", "s19433")
sqlContext = SQLContext(sc)

text_file = sc.textFile("/data/access_logs/access.log.2015-12-19")

ip_count = text_file.map(lambda line: line.split(" ")[0]).map(lambda ip: (ip, 1) if "7" in ip else (ip,0)).reduceByKey(lambda a, b: a + b)
df = ip_count.toDF(["ip", "value"])
df.sort(desc("value")).show(10)