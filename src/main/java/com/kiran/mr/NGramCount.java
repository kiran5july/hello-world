package com.kiran.mr;
/*
 * Count words in input file
 * hadoop jar km_java.jar com.kiran.mr.NGramCount /home/kiran/km/km_hadoop/data/data_ngram_count /home/kiran/km/km_hadoop_op/op_mr/ngramc1
 * 
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramCount {
         
  public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();
      private String sWord1 = new String();
      private String sWord2 = new String();
     //@override
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         StringTokenizer tokenizer = new StringTokenizer(line);

         while (tokenizer.hasMoreTokens()) {

        	 sWord2 = sWord1;
        	 sWord1 = tokenizer.nextToken();

        	 if(sWord1.length() > 0 && sWord2.length() > 0){
        		 word.set(sWord2+" "+sWord1);
        		 context.write(word, one);
        	 }
         }
     }
  } 

  public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
     public void reduce(Text key, Iterable<IntWritable> values, Context context) 
       throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
             sum += val.get();
         }
         context.write(key, new IntWritable(sum));
     }
  }
         
  public static void main(String[] args) throws Exception {
	if (args.length != 2) {
		System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <output dir path>\n", 
				NGramCount.class);
		//ToolRunner.printGenericCommandUsage(System.err);
		return;
	}
     Configuration conf = new Configuration();
     
      //Job job = Job.getInstance(conf, "N-Gram count");
      Job job = new Job(conf, "N-Gram count");
      job.setJarByClass(NGramCount.class);
      
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);
         
     job.setMapperClass(NGramMapper.class);
     job.setReducerClass(NGramReducer.class);
         
     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
         
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
     System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
         
 }