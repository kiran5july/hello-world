package com.kiran.mr;

/*
 * hadoop jar km_java.jar com.kiran.mr.EmpHighSal /km/data_emp_records "|" /km/op_erec_001
 * 1. Input file name with path in HDFS
 * 2. File delimiter
 * 3. Output directory in HDFS
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmpHighSal {


  public static class EmpSalMapper extends Mapper<Object, Text, Text, Text>{

    //private final static DoubleWritable dSal = new DoubleWritable(1);
    private Text word = new Text("KM");
    private String sFileDelim;
    private double dMaxSal;
    
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		sFileDelim = context.getConfiguration().get("FLDELIM");
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sSplit = line.split("\\"+sFileDelim);
        
        String sEmp = sSplit[0]+"-"+sSplit[1]+"="+sSplit[3];
        double dSal = Double.parseDouble(sSplit[3]);

        //double dMaxSal = Double.parseDouble(context.getConfiguration().get("MAXSAL"));
        if(dSal > dMaxSal){
        	//context.getConfiguration().set("MAXSAL", sSplit[3]);
        	dMaxSal = dSal;
        	context.write(word, new Text(sEmp));
        }
        	
    }
  }

  public static class EmpSalReducer extends Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      double dMaxSal = 0;
      String sEmpMaxSal = "";
      //double dMaxSal = Double.parseDouble(context.getConfiguration().get("MAXSAL"));
      
      for (Text val : values) {
    	  
    	  String[] sSplit = val.toString().split("\\=");
    	  
    	  if(Double.parseDouble(sSplit[1])>dMaxSal){
    		  sEmpMaxSal = sSplit[0];
    		  dMaxSal = Double.parseDouble(sSplit[1]);
    	  }
      }

      context.write(new Text(sEmpMaxSal), new Text(Double.toString(dMaxSal)));
    }
  }

  public static void main(String[] args) throws Exception {
	  if (args.length != 3) {
		  System.err.printf("KM USAGE ERROR: hadoop jar <jar name> %s <input fiel path> <file delimiter> <output dir path>\n", 
				  EmpSalMapper.class);
		  //ToolRunner.printGenericCommandUsage(System.err);
		  return;
	  }
	  
	  Configuration conf = new Configuration();
    //conf.set("fs.default.name", "hdfs://datanode1.localdomain:9000");
    //conf.set("mapred.job.tracker", "datanode1.localdomain:8021");
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    conf.set("FLDELIM", args[1]);
    //conf.set("MAXSAL", "0");
    
    Job job = Job.getInstance(conf, "Emp High Sal Finder");
    //Job job = new Job(conf, "Emp High Sal Finder");
    job.setJarByClass(EmpHighSal.class);
    
    job.setMapperClass(EmpSalMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    //job.setReducerClass(EmpSalReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}