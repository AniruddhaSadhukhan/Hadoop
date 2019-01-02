package org;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Smapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private Text sports = new Text();
	private IntWritable fans = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		
		String word[] = line.split(",");
		
		if(word!=null && word.length == 3)
		{
			try{
			sports.set(word[2].toLowerCase());
			context.write(sports, fans);
			}catch(Exception e){}
			
			
		}
	}

}
