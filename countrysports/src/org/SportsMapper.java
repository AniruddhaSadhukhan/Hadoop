package org;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SportsMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	private Text outkey=new Text();
	private IntWritable outval= new IntWritable(1);
	
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
			context.write(outkey, outval);
			
			
		}
	}
	

}
