package org;


	
	
	

	import java.io.IOException;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	//import org.apache.hadoop.mapred.FileOutputFormat;
	import org.apache.hadoop.mapreduce.*;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class SportsDriver {
		/**
		 * @param args
		 * @throws InterruptedException 
		 * @throws ClassNotFoundException 
		 */
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
			// TODO Auto-generated method stub
			Configuration conf= new Configuration();
			Job job = Job.getInstance(conf); //explanation required
			job.setJarByClass(SportsDriver.class);
			job.setMapperClass(SportsMapper.class);
			job.setReducerClass(SportsReducer.class);
			
			job.setPartitionerClass(SportsPartitioner.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(3);
			
			FileInputFormat.addInputPath(job, new Path("sportsdir"));
			FileOutputFormat.setOutputPath(job, new Path("sportsoutput1"));
			
			job.waitForCompletion(true);
			

		}

	}
