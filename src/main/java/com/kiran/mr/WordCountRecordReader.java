package com.kiran.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;


public class WordCountRecordReader {

	// Driver program
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <output dir path>\n", 
					WordCountRecordReader.class);
			//ToolRunner.printGenericCommandUsage(System.err);
			return;
		}

		Configuration conf = new Configuration(); 
		//Job job = new Job(conf, "word Count-RecordReader");
		Job job = Job.getInstance(conf, "word count Record Reader");
		
		job.setJarByClass(WordCountRecordReader.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//  FileInputFormat.addInputPath(job, new Path("/home/cloudera/Desktop/A.txt"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/Output"));
		job.waitForCompletion(true);
	}
}