package com.kiran.mr;

/*
 * 
 * hadoop jar km_java.jar com.kiran.mr.MinimalMRWithDefaults /km/data_customers /km/op_minmrdefaults_001
 * 1. Input file name with path in HDFS for Customer master
 * 2. Output directory in HDFS
 * 
 */

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.tools.rumen.JobBuilder;
import org.apache.hadoop.util.GenericOptionsParser;

public class MinimalMRWithDefaults extends Configured implements Tool {
	
	//@Override
	public int run(String[] args) throws Exception {

		//Job job = new Job(getConf());
		Job job = Job.getInstance(getConf(), "Minimal MR with Defaults");
		
		if (job == null) {
			return -1;
		}
		/*  //Defaults
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		*/
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJobName("Minimal MR with Defaults");
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			//printUsage(tool, "<input> <output>");
			System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input file path> <output dir path>\n", 
					MinimalMRWithDefaults.class);
			return;
		}
		int exitCode = ToolRunner.run(new MinimalMRWithDefaults(), args);
		System.exit(exitCode);
	}
	
	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}

}
