package wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf); //explanation required
		job.setJarByClass(WCDriver.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		//applying combiner class
		job.setCombinerClass(WCReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("wcdir"));
		FileOutputFormat.setOutputPath(job, new Path("wcoutput"));
		
		job.waitForCompletion(true);
		

	}

}
