package ani;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Mdriver 
{
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setMapperClass(Mmapper.class);
		job.setJarByClass(Mdriver.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);//whichever comes left of the first tab is the key and the remaining is the value
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path("stockout"));
		FileOutputFormat.setOutputPath(job, new Path("minmaxout"));
		
		job.waitForCompletion(true);

	}

}
