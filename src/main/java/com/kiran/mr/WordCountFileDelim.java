package com.kiran.mr;

/*
 * hadoop jar /home/km/km/km_hadoop/km_mr.jar \
 * com.kiran.mr.WordCountFileDelim \
 * /kmdata/wordcount/data_login_duration , /km_hadoop/op_mr/output_wc_delim1
 * 1. Input file name with path
 * 2. File delimiter
 * 3. Output directory in HDFS
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountFileDelim {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
      // public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String sWordKey;
    
	protected void setup(Context context) throws java.io.IOException, InterruptedException{

		sWordKey = context.getConfiguration().get("FLDELIM");	
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString(), sWordKey);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  if (args.length != 3) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <file delimiter> <output dir path>\n", 
				  WordCountFileDelim.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  return;
	  }
	  
	  Configuration conf = new Configuration();
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    conf.set("FLDELIM", args[1]);
    
    Job job = Job.getInstance(conf, "word count File Delimiter");
    //Job job = new Job(conf, "word count File Delimiter");
    
    job.setJarByClass(WordCountFileDelim.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}