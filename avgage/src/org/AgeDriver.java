package org;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeDriver {

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf); //explanation required
		job.setJarByClass(AgeDriver.class);
		job.setMapperClass(AgeMapper.class);
		job.setReducerClass(AgeReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("custsdir"));
		FileOutputFormat.setOutputPath(job, new Path("custsoutpust"));
		job.setNumReduceTasks(2);
		
		job.waitForCompletion(true);
		

	}

}
