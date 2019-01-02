package org;


	
	
	

	import java.io.IOException;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.Text;
	//import org.apache.hadoop.mapred.FileOutputFormat;
	import org.apache.hadoop.mapreduce.*;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class StockDriver {
		/**
		 * @param args
		 * @throws InterruptedException 
		 * @throws ClassNotFoundException 
		 */
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
			// TODO Auto-generated method stub
			Configuration conf= new Configuration();
			Job job = Job.getInstance(conf); //explanation required
			job.setJarByClass(StockDriver.class);
			job.setMapperClass(StockMapper.class);
			job.setReducerClass(StockReducer.class);
			
			//job.setPartitionerClass(SportsPartitioner.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(job, new Path("stocksdir"));
			FileOutputFormat.setOutputPath(job, new Path("stocksout"));
			
			try{
			
				job.waitForCompletion(true);
			}catch(Exception e){
				
				e.printStackTrace();
			}

		}

	}
