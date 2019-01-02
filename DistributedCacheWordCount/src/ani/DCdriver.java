package ani;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DCdriver 
{

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		Path cwpath = new Path("cw/comwords.txt");
		URI uri = cwpath.toUri(); //URI : any kind of locator
		
		job.addCacheFile(uri); //Distributed Cache is in action
		
		job.setJarByClass(DCdriver.class);
		job.setMapperClass(DCmapper.class);
		job.setReducerClass(DCreducer.class);
		job.setCombinerClass(DCreducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path("wcfolder"));//can be called multiple times
		
		FileOutputFormat.setOutputPath(job, new Path("wcoutDC"));//output path must be one
		//Output file if preexisted will not overwrite,throw Exception
		
		job.waitForCompletion(true);
	}

}
