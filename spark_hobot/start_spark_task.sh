hadoop fs -rm -r -f "/user/s19433/spark_lab$1_result"
spark-submit --master yarn-client "/home/gr194/s19433/spark_hobot/spark_$1.py"