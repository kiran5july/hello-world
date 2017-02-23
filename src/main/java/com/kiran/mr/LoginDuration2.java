package com.kiran.mr;

/*
 * 
 * hadoop jar km_java.jar com.kiran.mr.LoginDuration2 /km/data_login_duration /km/op_cpt_001
 * 1. Input file name with path in HDFS 
 * 2. Output directory in HDFS
 * 
 */
import java.io.IOException;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LoginDuration2 {
         
  public static class TimeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
     //private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();
      
     //@override
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] sSplit = line.split(",");
         
         long lTimeDiff = Long.parseLong(sSplit[2]) - Long.parseLong(sSplit[1]);
         
		 word.set(sSplit[0]);
		 context.write(word, new LongWritable(lTimeDiff));

     }
  } 
         
  public static class TimeReducer extends Reducer<Text, LongWritable, Text, Text> {
 
     public void reduce(Text key, Iterable<LongWritable> values, Context context) 
       throws IOException, InterruptedException {

         long sum = 0;
         for (LongWritable val : values) {
             sum += val.get();
         }
         
    	 Text sTime = new Text();
         sTime.set( sum + " ms. ~ ["+sum/(60*60)+ " Hrs. " + (sum/60)%60 + " Min. " +  sum%60 + " Sec.]");
         context.write(key, sTime);
     }
  }
         
  public static void main(String[] args) throws Exception {
	  if (args.length != 2) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <output dir path>\n", 
				  LoginDuration2.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  return;
		  }
     Configuration conf = new Configuration();
         
     //Job job = Job.getInstance(conf, "Login Duration 2");
     @SuppressWarnings("deprecation")
	Job job = new Job(conf, "Login Duration 2");
      job.setJarByClass(LoginDuration2.class);
      
      job.setMapOutputKeyClass(Text.class); 
      job.setMapOutputValueClass(LongWritable.class);
      
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);

     job.setMapperClass(TimeMapper.class);
     job.setReducerClass(TimeReducer.class);
         
     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
         
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
     
     job.waitForCompletion(true);
  }
         
 }