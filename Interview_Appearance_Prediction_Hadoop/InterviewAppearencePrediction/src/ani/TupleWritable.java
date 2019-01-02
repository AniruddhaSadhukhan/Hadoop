package ani;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class TupleWritable implements Writable 
{
	private IntWritable yes = new IntWritable();
	private IntWritable no= new IntWritable();
	
	public void set(int y,int n)
	{
		yes.set(y);
		no.set(n);
	}
	
	public int getNo()
	{
		return no.get();
	}
	
	public int getYes()
	{
		return yes.get();
	}
	
	@Override
	public void readFields(DataInput inpstream) throws IOException 
	{
			yes.readFields(inpstream);
			no.readFields(inpstream);
	}

	@Override
	public void write(DataOutput outstream) throws IOException 
	{
			yes.write(outstream);//read write order to be maintained
			no.write(outstream);
	}

	@Override
	public String toString() {
		return String.valueOf(yes.get())+"\t"+String.valueOf(no.get());
	}

}
