package com.kiran.mr;

/*
 * 
 * hadoop jar km_java.jar com.kiran.mr.JoinMapCustomerCountByState  /km/data_customers "|" /km/data_states "|" /km/opt_dc_001
 * 1. Input file name with path in HDFS for Customer master
 * 2. File1 delimiter
 * 3. Input file name with path in HDFS for State (Data to be cached)
 * 4. File2 delimiter
 * 5. Output directory in HDFS
 * 
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache; //old version
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.Counter;
//import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinMapCustomerCountByState {
	
	
	public static class CustomersMapper extends Mapper<Object, Text, Text, IntWritable> {
        
		
		private Map<String, String> oStateMap = new HashMap<String, String>();
		private Text outputKey = new Text();
		//private Text outputValue = new Text();
		private String sFileDelim;
		private String sDistCachFileDelim;
		
		@SuppressWarnings("deprecation")
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			//URI[] uri = context.getCacheFiles();
			Path[] files = context.getLocalCacheFiles();
			
			sFileDelim = context.getConfiguration().get("FLDELIM_IN");
			sDistCachFileDelim = context.getConfiguration().get("FLDELIM_DC");
			
			for (Path p : files) {
				//if (p.getName().equals("abc.dat")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\\"+sDistCachFileDelim);
						String sStCode = tokens[0];
						String sState = tokens[1];
						oStateMap.put(sStCode, sState);
						line = reader.readLine();
					}
					reader.close();
				//}
			}
			if (oStateMap.isEmpty()) {
				throw new IOException("Unable to load Abbrevation data.");
			}
		}

		
        protected void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
        	
        	String sRow = value.toString();
        	String[] tokens = sRow.split("\\"+sFileDelim);
        	String sStateCode = tokens[4];
        	String sState = oStateMap.get(sStateCode);
        	outputKey.set(sState);
        	
        	//Test to send customer details for value
        	//outputValue.set(sRow);
      	  	//context.write(outputKey, outputValue);
        	context.write(outputKey, new IntWritable(1));
        }  
}
	  public static class CustSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
	  Configuration conf = new Configuration();
	  //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  
	  if (args.length != 5) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input file> <file delimiter> <Join/DC File> <file demiliter> <output dir path>\n", JoinMapCustomerCountByState.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  //System.exit(2);
		  return;
	  }
	  String sInpFilePath = args[0];
	  conf.set("FLDELIM_IN", args[1]);
	  String sDistCachFilePath = args[2];
	  conf.set("FLDELIM_DC", args[3]);
	  
    Job job = Job.getInstance(conf, "Map Join/Distrib Cache Customers by State");
    //Job job = new Job(conf, "Map Join/Distrib Cache Customers by State");
    job.setJarByClass(JoinMapCustomerCountByState.class);
    //job.setNumReduceTasks(0);
    
    try{
    //DistributedCache.addCacheFile(new URI(sDistCachFilePath), job.getConfiguration()); //old version
    job.addCacheFile(new URI(sDistCachFilePath));
    
    job.setMapperClass(CustomersMapper.class);
    job.setReducerClass(CustSumReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(Text.class); //USE THIS FOR TESTING CUSTOMER DETAILS PASSED
    job.setMapOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(sInpFilePath));
    FileOutputFormat.setOutputPath(job, new Path(args[4]));
    
    job.waitForCompletion(true);
    
    }catch(Exception e){
    	System.out.println(e);
    } 
    
  }
}