package com.kiran.mr;

/*
 * Partition by Cust#
 * 
 * hadoop jar km_mr.jar com.kiran.mr.PartitionerCustomerTotalPurchase /km/data_customer_purchases "|" /km/op_cpt_001
 * 1. Input file name with path in HDFS
 * 2. File delimiter
 * 3. Output directory in HDFS
 * 
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;


public class PartitionerCustomerTotalPurchase {


  public static class CustPurchasesMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text word = new Text();
    private final static DoubleWritable dwPurchAmt = new DoubleWritable(1);
    private String sFileDelim;
 
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		sFileDelim = context.getConfiguration().get("FLDELIM");
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sSplit = line.split("\\"+sFileDelim);
        
        String sCust = sSplit[0]; //+"@"+sSplit[1]; //Append Store# if needed
        word.set(sCust);

        double dAmt = Double.parseDouble(sSplit[4]);
        dwPurchAmt.set(dAmt);
        
       	context.write(word, dwPurchAmt);
        	
    }
  }

  //Partitioner
	// Output types of Mapper should be same as arguments of Partitioner
	public static class PartitionByCustNum extends Partitioner<Text, DoubleWritable> {

		@Override
		public int getPartition(Text key, DoubleWritable value, int numPartitions) {
			String line = key.toString();
	        String[] sSplit = line.split("\\@");
			
	        long lCustNum = Long.parseLong(sSplit[0]);
	        
			int iPartition=0;
			
			if(lCustNum >= 1500000)
				iPartition=0;
			else if(lCustNum >= 1400000)
					iPartition=1;
			else if(lCustNum >= 1300000)
				iPartition=2;
			else
				iPartition=3;
			/*
			switch(key.val)
			{
				case (dAmt >= 100):
					iPartition=1; break;
				case (dAmt >= 50 ):
					iPartition=2; break;
				default:
					iPartition=1; break;
			}
			*/
			return(iPartition);
		}


	}
	
  public static class CustPurchasesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double dTotal = 0;
      
      for (DoubleWritable val : values) {
    	  dTotal+=val.get();
      }
      result.set(dTotal);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  
	  if (args.length != 3) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <file delimiter> <output dir path>\n", 
				  PartitionerCustomerTotalPurchase.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  //System.exit(2);
		  return;
	  }
	  
	  
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");

    conf.set("FLDELIM", args[1]);
    
    Job job = Job.getInstance(conf, "Customer Purchases Total using Partitioner");
    //Job job = new Job(conf, "Customer Purchases Total using Partitioner");
    
    job.setJarByClass(PartitionerCustomerTotalPurchase.class);
    
	// Forcing program to run 3 reducers
	job.setNumReduceTasks(4);
	
    job.setMapperClass(CustPurchasesMapper.class);
    //job.setCombinerClass(CustPurchasesReducer.class);
    job.setReducerClass(CustPurchasesReducer.class);
    job.setPartitionerClass(PartitionByCustNum.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}