package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AGEreducer extends Reducer<Text, TupleWritable, Text, DoubleWritable>
{
	private DoubleWritable avgage = new DoubleWritable();

	@Override
	protected void reduce(Text prof, Iterable<TupleWritable> ages_count,Context context)
			throws IOException, InterruptedException 
	{
		
		double sum=0,n=0;
		
		for(TupleWritable t : ages_count)
		{
			sum = sum + t.getAge();
			n=n+t.getCount();
		}
		sum=sum/n;
		avgage.set(sum);
		context.write(prof, avgage);
	}

}
