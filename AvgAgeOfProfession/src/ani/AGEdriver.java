package ani;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AGEdriver 
{

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(AGEdriver.class);
		job.setMapperClass(AGEmapper.class);
		job.setReducerClass(AGEreducer.class);
		job.setCombinerClass(AGEcombiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TupleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path("custsfolder"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("custsout"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		
		job.waitForCompletion(true);//starts processing
		
	}
	

}
