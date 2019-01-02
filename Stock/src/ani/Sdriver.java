package ani;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sdriver 
{

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(Sdriver.class);
		job.setMapperClass(Smapper.class);
		job.setReducerClass(Sreducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path("stocksdir"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("stockout"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		
		
		job.waitForCompletion(true);//starts processing
	}

}
