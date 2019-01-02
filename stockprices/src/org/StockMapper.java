package org;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;



public class StockMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
	private Text outkey=new Text();
	private DoubleWritable outval= new DoubleWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String record= value.toString();
		String fields[]=record.split(",");
		if(fields!=null && fields.length==3)
		{
			try{
				double price=Double.parseDouble(fields[2]);
				outkey.set(fields[0].toLowerCase());
				outval.set(price);
				context.write(outkey, outval);
			}catch(Exception e){
				
				
			}
			
		}
	}
	

}
