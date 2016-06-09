from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
import sys
from datetime import datetime as dt

log_format = re.compile( 
    r"(?P<host>[\d\.]+)\s" 
    r"(?P<identity>\S*)\s" 
    r"(?P<user>\S*)\s"
    r"\[(?P<time>.*?)\]\s"
    r'"(?P<request>.*?)"\s'
    r"(?P<status>\d+)\s"
    r"(?P<bytes>\S*)\s"
    r'"(?P<referer>.*?)"\s'
    r'"(?P<user_agent>.*?)"\s*'
)

def parseLine(line):
    match = log_format.match(line)
    if not match:
        return ("", "", "", "", "", "", "" ,"")

    request = match.group('request').split()
    return (match.group('host'), match.group('time').split()[0], \
       request[0], request[1], match.group('status'), match.group('bytes'), \
        match.group('referer'), match.group('user_agent'),
        dt.strptime(match.group('time').split()[0], '%d/%b/%Y:%H:%M:%S').hour)

sc = SparkContext("yarn-client", "s19433")
sqlContext = SQLContext(sc)

text_file = sc.textFile(sys.argv[1])
#text_file = sc.textFile("/data/access_logs/spark/access.log.2015-12-19")
#text_file = sc.textFile("/user/s19433/small.log")

def get_ip_and_data_size(line):
    data = parseLine(line)
    if not data[5]:
    	return ('blank',0)
    return (data[0], int(data[5]))

ip_count = text_file.map(lambda line: get_ip_and_data_size(line)).reduceByKey(lambda a, b: a + b)
df = ip_count.toDF(["ip", "value"]).sort(desc("value"))
df.show()
#df.write.save("spark_lab2_result")