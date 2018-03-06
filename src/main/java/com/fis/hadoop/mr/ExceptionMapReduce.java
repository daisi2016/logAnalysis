package com.fis.hadoop.mr;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ExceptionMapReduce {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			if(line.contains("Exception:")) {
				word.set(line);
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	
	 public static void main(String[] args) throws Exception {
		 List<File>list = new ArrayList<File>();
		   getAllFile(new File("E:\\data\\in"), list);
		 
		 System.setProperty("HADOOP_USER_NAME", "root");
		 for(File file:list){
	        JobConf conf = new JobConf(ExceptionMapReduce.class);
	        
	        conf.setJobName("exception count");

	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(IntWritable.class);

	        conf.setMapperClass(Map.class);
	        conf.setCombinerClass(Reduce.class);
	        conf.setReducerClass(Reduce.class);

	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(conf, new Path(file.getAbsolutePath()));
	        FileOutputFormat.setOutputPath(conf,new Path("E:\\data\\"+"\\out\\result"));

	        RunningJob job = JobClient.runJob(conf);
	        
	        while (!job.isComplete()) {  
	            job.waitForCompletion();  
	        }  
		 }
		
	    } 
	 
	 private  static void getAllFile(File file,List<File>list){
		 if (file.isDirectory()) {
				File files[] = file.listFiles();
				for (File fil : files) {
						getAllFile(fil,list);
				}
			} else if (file.isFile()) {
				list.add(file);			 
			}
	 }
	 
	 

}
