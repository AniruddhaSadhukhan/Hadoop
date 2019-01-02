package org;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;



public class SportsMapper extends Mapper<LongWritable,Text,Text,Text> {
	private Text outkey=new Text();
	private Text outval= new Text();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String record= value.toString();
		String fields[]=record.split(",");
		if(fields!=null && fields.length==3)
		{
			String sports=fields[2].toLowerCase();
			outkey.set(sports);
			outval.set(fields[2].toLowerCase());
			context.write(outkey, outval);
			
			
		}
	}
	

}
