package ani;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class Pair implements WritableComparable<Pair>
{
	private Text sports = new Text(),country = new Text();
	
	public void set(String s,String c)
	{
		sports.set(s);
		country.set(c);
	}
	
	public String getSports()
	{
		return sports.toString();
	}
	
	public String getCountry()
	{
		return country.toString();
	}
	
	@Override
	public String toString() 
	{
		String s = "["+sports.toString()+","+country.toString()+"]";	
		return s;
	}

	@Override
	public void readFields(DataInput din) throws IOException 
	{
			sports.readFields(din);
			country.readFields(din);
	}

	@Override
	public void write(DataOutput dout) throws IOException 
	{
			sports.write(dout);
			country.write(dout);
	}

	@Override
	public int compareTo(Pair o) 
	{
			int a = sports.compareTo(o.sports);
			
			if(a == 0)
			{
				a = country.compareTo(o.country);
			}
			return a;
	}

	@Override
	public int hashCode() 
	{
		int hash = sports.hashCode()*31 + country.hashCode()*31; //for equal distribution multiply by 2^5-1
		return hash;
	}
	
	

}
