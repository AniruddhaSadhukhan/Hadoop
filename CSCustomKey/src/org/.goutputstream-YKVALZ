package org;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sdriver 
{

	
	public static void main(String[] args) throws IOException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(Sdriver.class);
		job.setMapperClass(Smapper.class);
		job.setReducerClass(Sreducer.class);
		job.setCombinerClass(Sreducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path("sports"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("sout"));//output path must be one
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
