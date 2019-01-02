package ani;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pdriver 
{

	
	public static void main(String[] args) throws IOException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(Pdriver.class);
		job.setMapperClass(Pmapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path("custsdir"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("custout2"));//output path must be one
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
