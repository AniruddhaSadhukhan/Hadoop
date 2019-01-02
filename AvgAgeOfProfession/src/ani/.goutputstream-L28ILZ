package ani;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Writable;

public class TupleWritable implements Writable 
{
	private IntWritable count = new IntWritable();
	private DoubleWritable age= new DoubleWritable();
	
	public void set(double a,int c)
	{
		age.set(a);
		count.set(c);
	}
	
	public double getAge()
	{
		return age.get();
	}
	
	public int getCount()
	{
		return count.get();
	}
	
	@Override
	public void readFields(DataInput inpstream) throws IOException 
	{
			age.readFields(inpstream);
			count.readFields(inpstream);
	}

	@Override
	public void write(DataOutput outstream) throws IOException 
	{
			age.write(outstream);//read write order to be maintained
			count.write(outstream);
	}

}
