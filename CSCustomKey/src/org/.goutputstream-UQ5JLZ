package org;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CSPair implements WritableComparable<CSPair> {
	
	//Writable
	//a)readFields()
	//b)write()
	//WritableComparable extends Serializable,Writable
	//a)int compareTo()

	
	private  Text sports = new Text(), country = new Text();
	public void set(String s,String c){
		
		sports.set(s);
		country.set(c);
		
	}
	
	public String getSports(){
		
		return sports.toString();
	}
	
	public String getCountry(){
		
		return country.toString();
	}
	
	public String toString(){
		
		String r = "["+sports.toString()+","+country.toString()+"]";
		return r;
		
	}
	@Override
	
	public int compareTo(CSPair o) {
	
		int c = sports.compareTo(o.sports);
		if(c == 0){
			
			c = country.compareTo(o.country);
		}
		
		return c;
	}

	@Override
	public void readFields(DataInput din) throws IOException {
		
		sports.readFields(din);
		country.readFields(din);
		
	}

	//CSPair obj = new CSPair()
	//DataOutputStresam dout = new DataOutputStream(new ......)
	//obj.write(dout)
	@Override
	public void write(DataOutput dataout) throws IOException {
		sports.write(dataout);
		country.write(dataout);
		
		
	}

	
}
