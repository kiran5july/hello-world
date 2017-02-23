package com.kiran.mr;

/*
 * hadoop jar /home/km/km/km_hadoop/km_mr.jar com.kiran.mr.WordCountByKey /kmdata/wordcount/data_wordcount /kmdata/wordcount/output_wc_key/output003 we
 * 1. Input file with path
 * 2. Output directory
 * 3. Word to search for
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

public class WordCountByKey {
         
  public static class WCKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();
     private String sWordKey;
     
		protected void setup(Context context) throws java.io.IOException, InterruptedException{

			sWordKey = context.getConfiguration().get("WORD");	
		}
		
     //@override
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         StringTokenizer tokenizer = new StringTokenizer(line);
         String sCurrentToken;
         
         //if(sWordKey != null){
	         while (tokenizer.hasMoreTokens()) {
	        	 sCurrentToken = tokenizer.nextToken().trim();
	        	 if(sCurrentToken.equals(sWordKey)) {
	        		 word.set(sCurrentToken);
	        		 context.write(word, one);
	        	 }//end if
	         }//end while
         //}//end if
     }
  } 
         
  public static class WCKeyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
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
	if (args.length != 3) {
		System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <Search string> <output dir path>\n", 
				WordCountByKey.class);
		//ToolRunner.printGenericCommandUsage(System.err);
		return;
	}


     Configuration conf = new Configuration();
     //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
      //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
     conf.set("WORD", args[1]);
     
     //@SuppressWarnings("deprecation")
	//Job job = new Job(conf);
     //job.setJobName("word count By Key");
      Job job = Job.getInstance(conf, "word count By Key");
      //Job job = new Job(conf, "word count By Key");
      
     job.setJarByClass(WordCountByKey.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);
         
     job.setMapperClass(WCKeyMapper.class);
     job.setReducerClass(WCKeyReducer.class);
         
     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
         
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[2]));

     System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
         
 }