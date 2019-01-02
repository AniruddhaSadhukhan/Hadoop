package ani;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class IAReducer extends Reducer<Text, TupleWritable, Text, TupleWritable>
{
	private TupleWritable outval = new TupleWritable();
	@Override
	protected void reduce(Text key, Iterable<TupleWritable> values,Context context)
			throws IOException, InterruptedException 
	{
		int sumy = 0,sumn = 0;
		for(TupleWritable t : values)
		{
			sumy = sumy + t.getYes();
			sumn = sumn + t.getNo();
		}
		outval.set(sumy,sumn);
		context.write(key, outval);
	}
	
}
