package com.kiran.mr;

/*
 * 
 * hadoop jar km_java.jar com.kiran.mr.JoinReduceCustomerTotalPurchase /km/data_customers /km/data_customer_purchases /km/op_cpt_001
 * 1. Input file name with path in HDFS for Customer master
 * 2. Input file name with path in HDFS for Customer purchases
 * 3. Output directory in HDFS
 * Default file delimiter is pipe (|)
 */


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;


public class JoinReduceCustomerTotalPurchase {

	public static class CustomersMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
        protected void map(Object key, Text value, Context context) throws java.io.IOException, InterruptedException {
        	
        	String sRow = value.toString();
        	String[] sSplit = sRow.split("\\|");
        	String sCustNum = sSplit[0];
        	outputKey.set(sCustNum);

        	outputValue.set("Cust:"+sSplit[1]+","+sSplit[2]+","+sSplit[3]+","+sSplit[4]);
        	//context.write(new TextPair(sCustNum, "0"),outputValue);
        	context.write(outputKey, outputValue);
        }  
}

  public static class CustPurchasesMapper extends Mapper<Object, Text, Text, Text>{

		private Text outputKey = new Text();
		private Text outputValue = new Text();
 
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sSplit = line.split("\\|");
        
        String sCustNum = sSplit[0]; //+"@"+sSplit[1]; //Append Store# if needed
        outputKey.set(sCustNum);

        outputValue.set("Txns:"+sSplit[4]);
      //context.write(new TextPair(sCustNum, "1"), outputValue);
       	context.write(outputKey, outputValue);
        	
    }
  }

	
  public static class ReduceJoinCustPurchasesReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
		String sName = "";
		double dTotal = 0.0;
		int iCount = 0;
		
		for (Text t : values) {
			String parts[] = t.toString().split("\\:");  //Delimiter is used in mapper to separate out record type
			if (parts[0].equals("Txns")) {
				iCount++;
				dTotal += Float.parseFloat(parts[1]);
			} else if (parts[0].equals("Cust")) {
				sName = parts[1];
			}
		}
		String str = String.format("%d\t%f", iCount, dTotal);
		context.write(new Text(key.toString()+"("+sName+")"), new Text(str));
    }
  }
  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  
	  if (args.length != 3) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <file delimiter> <output dir path>\n", JoinReduceCustomerTotalPurchase.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  //System.exit(2);
		  return;
	  }
	  
	  
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");

    Job job = Job.getInstance(conf, "Customer Purchases Total using Reduce side join");
    //Job job = new Job(conf, "Customer Purchases Total using Reduce side join");
    job.setJarByClass(JoinReduceCustomerTotalPurchase.class);
    

    job.setMapperClass(CustomersMapper.class);
    job.setMapperClass(CustPurchasesMapper.class);
    //job.setCombinerClass(ReduceJoinCustPurchasesReducer.class);
    job.setReducerClass(ReduceJoinCustPurchasesReducer.class);
    //job.setPartitionerClass(PartitionByCustNum.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
	MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustomersMapper.class);
	MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, CustPurchasesMapper.class);
	
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}