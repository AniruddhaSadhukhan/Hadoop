package ani;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class Pair implements WritableComparable<Pair>
{

	private Text name = new Text();
	private DoubleWritable price = new DoubleWritable();
	
	public void set(String s,double p)
	{
		name.set(s);
		price.set(p);
	}
	
	
	public Text getName() {
		return name;
	}


	public DoubleWritable getPrice() {
		return price;
	}

	
	
	
	@Override
	public int hashCode() 
	{
		return name.hashCode()*31 + price.hashCode()*31;
	}


	@Override
	public String toString() 
	{
				return "["+name+","+price+"]";
	}


	@Override
	public void readFields(DataInput din) throws IOException 
	{
				name.readFields(din);
				price.readFields(din);
	}

	@Override
	public void write(DataOutput dout) throws IOException 
	{
				name.write(dout);
				price.write(dout);
	}

	@Override
	public int compareTo(Pair o) 
	{
				int c = name.compareTo(o.name);
				if(c==0)
				{
					c= price.compareTo(o.price);
				}
				return c;
	}

}
