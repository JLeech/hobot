package ru.mipt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.lang.InterruptedException;
import java.io.IOException;

public class UserCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UserCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf);
        job.setJarByClass(UserCount.class);
        job.setMapperClass(UserCountMapper.class);
        job.setReducerClass(UserCountReducer.class); // = IntSumReducer.class

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(8);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class UserCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text uid = new Text();

        @Override
        public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException 
        {
            String [] fields = line.toString().split("\t");
            System.err.println("System.err.println:" + fields[2]);
            uid.set(fields[2]);
            
            context.write(uid, one);
        }
    }

    public static class UserCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text uid, Iterable<IntWritable> values, Context context)
            throws IOException,InterruptedException
        {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }

            context.write(uid, new IntWritable(sum));
       }
    }
}
