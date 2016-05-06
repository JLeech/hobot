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

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableExistsException;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import java.lang.InterruptedException;
import java.io.IOException;


public class HbaseFiller extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HbaseFiller(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf);
        job.setJarByClass(HbaseFiller.class);
        job.setMapperClass(HbaseFillerMapper.class);
        //TableMapReduceUtil.initTableReducerJob(TABLE, HbaseFillerReducer.class, job);
        job.setReducerClass(HbaseFillerReducer.class); // = IntSumReducer.class

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setNumReduceTasks(1);

        prepareHbase();

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public void prepareHbase() {
        Configuration conf = HBaseConfiguration.create();
        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();    

            TableName tableName = TableName.valueOf("table_19433");
            if (admin.tableExists(tableName)){
                System.err.println("table exists " + tableName.toString() + " - deleting");
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            System.err.println("table not exists " + tableName.toString());
            createTable(admin, tableName, null, "colfam");

        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void createTable(Admin admin, TableName table, byte[][] splitKeys, String... colfams) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(table);
        for (String cf : colfams) {
            HColumnDescriptor coldef = new HColumnDescriptor(cf);
            desc.addFamily(coldef);
        }
        if (splitKeys != null) {
            admin.createTable(desc, splitKeys);
        } else {
            admin.createTable(desc);
        }
    }

    public static class HbaseFillerMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text domain = new Text();
        private Text date_text = new Text();

        @Override
        public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
            String [] fields = line.toString().split("\t");
            System.err.println("System.err.println:" + fields[2]);
            String right_timestamp = fields[1] + "000";

            SimpleDateFormat date_formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date(Long.parseLong(right_timestamp));
            date_text.set(date_formatter.format(date));
            try{
              URI uri = new URI(fields[2]);
              String domain_name = uri.getHost();
              String domain_no_www = domain_name.startsWith("www.") ? domain_name.substring(4) : domain_name;
              domain.set(domain_no_www);
              //Text timestamp_domain = new Text(date_text + ":" + domain);
              Text domain_count = new Text( domain ); 
              context.write(date_text, one);
            }catch(URISyntaxException e){
              e.printStackTrace();
            }
        }
    }

    public static class HbaseFillerReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException,InterruptedException{
            
            Map<String, Integer> domain_count = new HashMap<String, Integer>();

            for (Text value: values) {
              String domain = value.toString();
              if domain_count.containsKey(domain){
                domain_count.put(domain, domain_count.get(domain) + 1 )
              }else{
                domain_count.put(domain, 1 )
              }
            }

            String domains = new String();
            for (Map.Entry entry : domain_count.entrySet()) {
              System.out.println("Key: " + entry.getKey() + " Value: " + entry.getValue());
            }

            String[] hbase_data = key.toString().split(":");
            Text output_value = new Text(hbase_data[1] + " " + sum); 
            //writeToHbase(key, sum);
            context.write(key, output_value);
       }

       // public void writeToHbase(Text key, int value){
       //      Configuration conf = HBaseConfiguration.create();
       //      TableName tableName = TableName.valueOf("table_19433");
       //      try{
       //          HTable table = new HTable(conf, tableName);
       //          String[] hbase_data = key.toString().split(":");
       //          Put put = new Put(Bytes.toBytes(hbase_data[0]));
       //          put.add(Bytes.toBytes("colfam"), Bytes.toBytes("value"), Bytes.toBytes(value) );
       //          put.add(Bytes.toBytes("colfam"), Bytes.toBytes("address"), Bytes.toBytes(value) );
       //          table.put(put);
       //      }catch(IOException e){
       //          e.printStackTrace();
       //      }
       // }
    }
}
