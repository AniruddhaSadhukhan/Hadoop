package ani;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IADriver 
{

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(IADriver.class);
		job.setMapperClass(IAMapper.class);
		job.setReducerClass(IAReducer.class);
		job.setCombinerClass(IAReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TupleWritable.class);
		
		
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path("datasets"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("ProbabilitiesLists"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		
		job.waitForCompletion(true);//starts processing
		
	}
	

}
