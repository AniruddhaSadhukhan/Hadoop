package test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ani.IADriver;
import ani.IAMapper;
import ani.IAReducer;
import ani.TupleWritable;

public class TestDriver 
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		/*Path cwpath = new Path("ProbabilitiesLists/");
		URI uri = cwpath.toUri(); //URI : any kind of locator
		*/
		job.addCacheFile(new URI("/user/edureka/ProbabilitiesLists/part-r-00000")); //Distributed Cache is in action
		job.addCacheFile(new URI("/user/edureka/ProbabilitiesLists/part-r-00001"));
		job.addCacheFile(new URI("/user/edureka/ProbabilitiesLists/part-r-00002"));
		
		job.setJarByClass(TestDriver.class);
		job.setMapperClass(TestMapper.class);
		//job.setReducerClass(TestReducer.class);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path("datasets"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("OutputMatches"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		
		job.waitForCompletion(true);
	}

}
