package org;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class SPkey implements WritableComparable<SPkey>{

	
	private Text stock= new Text();
	private DoubleWritable price= new DoubleWritable();
	
	public void set(String s,double p){
		
		stock.set(s);
		price.set(p);
	}
	
	public String getStock(){
		return stock.toString();
	}
	
	public double getPrice(){
		
		return price.get();
	}
	
	public int hashCode(){
		
		return stock.hashCode()*31+price.hashCode();
	}
	
	public String toString(){
		return "["+stock.toString()+","+price.toString()+"]";
	}
	@Override
	public void readFields(DataInput datain) throws IOException {
		stock.readFields(datain);
		price.readFields(datain);
	}

	@Override
	public void write(DataOutput dataout) throws IOException {
		stock.write(dataout);
		price.write(dataout);
		
	}

	@Override
	public int compareTo(SPkey o) {
		int c= stock.compareTo(o.stock);
		if(c==0){
			c=price.compareTo(o.price);
		}
		return c;
	}
	
	

}
