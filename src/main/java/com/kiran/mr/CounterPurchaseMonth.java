package com.kiran.mr;

/*
 * 
 * hadoop jar km_java.jar com.kiran.mr.CounterPurchaseMonth /km/data_customer_purchases /km/op_emp_001
 * 1. Input file name with path in HDFS for Customer master
 * 2. Input file name with path in HDFS for Customer purchases
 * 3. Output directory in HDFS
 * 
 */

import java.io.IOException;
import java.util.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterPurchaseMonth {
	
	public static enum MONTH {
		JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC, INVALID
	};
	
	public static class CustPurchasesMapper extends Mapper<Object, Text, Text, Text> {
        private Text txtKey = new Text();
        private Text txtVal = new Text();
        
        protected void map(Object key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	String line = value.toString();
        	String[] aStr = line.split("\\|");
        	
        	String sCustNum = aStr[0];
        	txtKey.set(sCustNum);
        	
        	String sDate = aStr[2];
        	long lts = Long.parseLong(sDate);
        	Date dt = new Date(lts);
        	/*txtVal.set((time.getMonth()+1)+"/"+time.getDate()+"/"+time.getYear()+" "+time.getHours()+":"+time.getMinutes()+":"+time.getSeconds()+" "+time.getTimezoneOffset());*/
        	String sDateFormatted = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss Z").format(dt);
        	txtVal.set(sDateFormatted);
        	
        	LocalDate localDate = dt.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        	int m = localDate.getMonthValue();
        	//int m = time.getDate();
        	switch(m)
        	{
	        	case 0: context.getCounter(MONTH.JAN).increment(1);	break;
	        	case 1: context.getCounter(MONTH.FEB).increment(1); break;
	        	case 2: context.getCounter(MONTH.MAR).increment(1);	break;
	        	case 3: context.getCounter(MONTH.APR).increment(1);	break;
	        	case 4: context.getCounter(MONTH.MAY).increment(1);	break;
	        	case 5: context.getCounter(MONTH.JUN).increment(1);	break;
	        	case 6: context.getCounter(MONTH.JUL).increment(1);	break;
	        	case 7: context.getCounter(MONTH.AUG).increment(1);	break;
	        	case 8: context.getCounter(MONTH.SEP).increment(1);	break;
	        	case 9: context.getCounter(MONTH.OCT).increment(1);	break;
	        	case 10: context.getCounter(MONTH.NOV).increment(1); break;
	        	case 11: context.getCounter(MONTH.DEC).increment(1);break;
	        	default: context.getCounter(MONTH.INVALID).increment(1);break;
        	}
        	
        	context.write(txtKey, txtVal);
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <output dir path>\n", CounterPurchaseMonth.class);
			//ToolRunner.printGenericCommandUsage(System.err);
			return;
		}
		
	Configuration conf = new Configuration();
	//conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
	//conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
    //Job job = new Job();
    Job job = Job.getInstance(conf, "Counter Purchase by Month");
    
    job.setJarByClass(CounterPurchaseMonth.class);
    job.setJobName("KM Counter Test");
    job.setNumReduceTasks(0);
    
    job.setMapperClass(CustPurchasesMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    //output Job Counters
    Counters counters = job.getCounters();
    
    Counter c1;
    c1 = counters.findCounter(MONTH.JAN);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.FEB);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.MAR);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.APR);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.MAY);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.JUN);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.JUL);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.AUG);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.SEP);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.OCT);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.NOV);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.DEC);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
    c1 = counters.findCounter(MONTH.INVALID);
    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
  }
}