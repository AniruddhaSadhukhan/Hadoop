package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AGEcombiner extends Reducer<Text, TupleWritable, Text, TupleWritable>
{
	private TupleWritable outtuple = new TupleWritable();

	@Override
	protected void reduce(Text prof, Iterable<TupleWritable> ages_count,Context context)
			throws IOException, InterruptedException 
	{
		
		double sum=0;
		int n=0;
		
		for(TupleWritable t : ages_count)
		{
			sum = sum + t.getAge();
			n=n+t.getCount();
		}
		
		outtuple.set(sum,n);
		context.write(prof, outtuple);
	}

}
