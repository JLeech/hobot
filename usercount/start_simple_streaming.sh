hadoop fs -rmdir /user/s19433/out_streaming_py
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files mapper.py,reducer.py -D mapreduce.job.reduces=1 -input /data/user_events_part -output /user/s19433/out_streaming_py -mapper mapper.py -reducer reducer.py
