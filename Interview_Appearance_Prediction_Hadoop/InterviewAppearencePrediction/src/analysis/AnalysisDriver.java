package analysis;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ani.IADriver;
import ani.IAMapper;
import ani.IAReducer;
import ani.TupleWritable;

public class AnalysisDriver 
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(AnalysisDriver.class);
		job.setMapperClass(AnalysisMapper.class);
		job.setReducerClass(AnalysisReducer.class);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path("OutputMatches"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("Analysis"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		
		job.waitForCompletion(true);
	}

}
