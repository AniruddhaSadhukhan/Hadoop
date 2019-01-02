package org;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SportsReducer extends Reducer<Text,Text,Text,IntWritable>{
	
	private IntWritable outval= new IntWritable();
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(Text val:values)
		{
			sum=sum+1;
		}
		outval.set(sum);
		context.write(key, outval);
		
	}
}

