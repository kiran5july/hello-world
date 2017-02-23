package com.kiran.mr;

/*
 * hadoop jar km_java.jar com.kiran.mr.CustomerTotalPurchases /km/data_customer_purchases "|" /km/op_cpt_001
 * 1. Input file name with path in HDFS
 * 2. File delimiter
 * 3. Output directory in HDFS
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerTotalPurchases {


  public static class CustPurchasesMapper extends Mapper<Object, Text, Text, FloatWritable>{

    private Text word = new Text();
    private final static FloatWritable dwPurchAmt = new FloatWritable(1);
    private String sFileDelim;
 
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		sFileDelim = context.getConfiguration().get("FLDELIM");
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sSplit = line.split("\\"+sFileDelim);
        
        String sCust = sSplit[0]; //+" @ "+sSplit[1];  //Append Store#
        word.set(sCust);

        float dAmt = Float.parseFloat(sSplit[4]);
        dwPurchAmt.set(dAmt);
        
       	context.write(word, dwPurchAmt);
        	
    }
  }

  public static class CustPurchasesReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float dTotal = 0;
      
      for (FloatWritable val : values) {
    	  dTotal+=val.get();
      }
      result.set(dTotal);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  if (args.length != 3) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <file delimiter> <output dir path>\n", CustomerTotalPurchases.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  return;
	  }
	  
	  Configuration conf = new Configuration();
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    conf.set("FLDELIM", args[1]);
    
    Job job = Job.getInstance(conf, "Customer Purchases Total");
    //Job job = new Job(conf, "Customer Purchases Total");
    job.setJarByClass(CustomerTotalPurchases.class);
    
    job.setMapperClass(CustPurchasesMapper.class);
    //job.setCombinerClass(CustPurchasesReducer.class);
    job.setReducerClass(CustPurchasesReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}