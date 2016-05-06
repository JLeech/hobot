from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext


sc = SparkContext("yarn-client", "s19433")
sqlContext = SQLContext(sc)

def get_ip_page(line):
	line_data = line.split("\"")
	if len(line_data) < 2:
		return ("blank", "blank")
	ip = line_data[0].split(" ")[0]
	page = line_data[1].split(" ")[1]
	return(ip, page)

text_file = sc.textFile("/data/access_logs/access.log.2015-12-19")
#text_file = sc.textFile("/user/s19433/small.log")
ip_page_df = text_file.map(lambda line: get_ip_page(line) ).toDF(["ip","page"])

most_active_ips_rows = ip_page_df.groupby("ip").count().sort(desc("count")).collect()[0:100]
most_active_ips = map(lambda row: row.ip, most_active_ips_rows)
visited_pages = ip_page_df[ip_page_df.ip.isin(most_active_ips)]

visited_pages_count = visited_pages.groupby("page").count().sort(desc("count")).show(25)
