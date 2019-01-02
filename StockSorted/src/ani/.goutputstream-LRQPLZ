package ani;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Sreducer extends Reducer<Pair, DoubleWritable, Text, Text>
{
	private Text outval = new Text();
	@Override
	protected void reduce(Pair key, Iterable<DoubleWritable> values,Context context)
			throws IOException, InterruptedException 
	{
		
		StringBuilder sb = new StringBuilder();
		for (DoubleWritable val:values)
		{
			sb.append(val);
			sb.append(",");
		}
		
		outval.set(sb.toString());
		context.write(key.getName(), outval);
	}
	
}
