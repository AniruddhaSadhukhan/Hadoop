package org;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SportsReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	private IntWritable outval= new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable val:values)
		{
			sum=sum+val.get();
		}
		outval.set(sum);
		context.write(key, outval);
		
	}
}

