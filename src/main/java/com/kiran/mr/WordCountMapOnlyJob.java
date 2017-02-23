package com.kiran.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapOnlyJob {

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



  public static void main(String[] args) throws Exception {
	if (args.length != 2) {
		System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <output dir path>\n", 
				WordCountMapOnlyJob.class);
		//ToolRunner.printGenericCommandUsage(System.err);
		return;
	}
	  
    Configuration conf = new Configuration();
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    //@SuppressWarnings("deprecation")
    Job job = Job.getInstance(conf, "KM Word Count Map Only");
    //Job job = new Job(conf, "KM Word Count Map Only");
    
    job.setJarByClass(WordCountMapOnlyJob.class);
    job.setMapperClass(WordCountMapOnlyJob.TokenizerMapper.class);

    
    // set output key type
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(IntWritable.class);
    
    //FileInputFormat.addInputPath(job, new Path("/home/cloudera/Desktop/A.txt"));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}