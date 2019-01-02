package ani;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Sreducer1 extends Reducer<Text, Text, Text, IntWritable>
{
	private IntWritable outval = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException 
	{
		//(cricket,{india,australia,india,india...,australia})
		
		int sum = 0;
		for(Text val: values)
		{
			sum=sum+1;
		}
		outval.set(sum);
		context.write(key, outval);
	}
	
}
