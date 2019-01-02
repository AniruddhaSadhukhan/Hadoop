package org;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class AgeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	private DoubleWritable outval= new DoubleWritable();
	private Text outkey= new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String record = value.toString().trim();
		String fields[] = record.split(",");
		if(fields != null && fields.length == 5){
			try{
				String prof=fields[4].toLowerCase();
				double age= Double.parseDouble(fields[3]);
				outkey.set(prof);
				outval.set(age);
				context.write(outkey, outval);
				
			}catch(Exception e){}
			
			
		}
	}

	
}
