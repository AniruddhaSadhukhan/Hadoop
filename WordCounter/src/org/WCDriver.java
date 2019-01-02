package org;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver 
{

	
	public static void main(String[] args) throws IOException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(WCDriver.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setCombinerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path("wcfolder"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("wcout"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		
		try 
		{
			job.waitForCompletion(true);//starts processing
		} 
		catch (ClassNotFoundException e) 
		{
						e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
						e.printStackTrace();
		}
	}

}
