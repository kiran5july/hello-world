package com.kiran.mr;

/*
 * Count words in input file
 * hadoop jar km_java.jar com.kiran.mr.WordCount /km/data_wordcount /km/opt_wc_cmprs_001
 * 
 * Use below if output file is compressed (gzip) file
 * hdfs dfs -copyToLocal /km/opt_wc_cmprs_001/part-r-00000.gz
 * uncompress using: gzip -k part-r-00000.gz
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.compress.GzipCodec;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
      // public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	String sStr = itr.nextToken().toUpperCase();
    	sStr = sStr.replaceAll("[^a-zA-Z0-9_-]+","");
        word.set(sStr); // set word as each input keyword
        context.write(word, one); // create a pair <keyword, 1>
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
	if (args.length != 2) {
		System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <output dir path>\n", 
				WordCount.class);
		//ToolRunner.printGenericCommandUsage(System.err);
		return;
	}
	
    Configuration conf = new Configuration();
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    //@SuppressWarnings("deprecation")
    //Job job = Job.getInstance(conf, "KM Word Count");
    Job job = new Job(conf, "KM Word Count");
    job.setJarByClass(WordCount.class);
    
    job.setMapperClass(WordCount.TokenizerMapper.class);
    //job.setCombinerClass(WordCount.IntSumReducer.class);
    job.setReducerClass(WordCount.IntSumReducer.class);
    
    // set output key type
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(IntWritable.class);
    
    //FileInputFormat.addInputPath(job, new Path("/home/cloudera/Desktop/A.txt"));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //Set compressed output
    //FileOutputFormat.setCompressOutput(job, true);
    //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}