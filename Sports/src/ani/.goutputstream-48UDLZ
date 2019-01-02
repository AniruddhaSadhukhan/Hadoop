package ani;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sdriver1 
{

	
	public static void main(String[] args) throws IOException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(Sdriver1.class);
		job.setMapperClass(Smapper1.class);
		job.setReducerClass(Sreducer1.class);
		//job.setCombinerClass(Sreducer1.class);//mapper output and reducer output must be same
		job.setPartitionerClass(Spartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path("sports"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("sout1"));//output path must be one
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
