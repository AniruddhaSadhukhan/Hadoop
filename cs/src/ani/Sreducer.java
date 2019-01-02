package ani;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Sreducer extends Reducer<Pair, IntWritable, Pair, IntWritable>
{
	private IntWritable outval = new IntWritable();
	@Override
	protected void reduce(Pair key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException 
	{
		
		int sum = 0;
		for (IntWritable val:values)
		{
			sum = sum+val.get();
		}
			
		
		outval.set(sum);
		context.write(key, outval);
	}
	
}
