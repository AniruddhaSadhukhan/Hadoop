package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Smapper extends Mapper<LongWritable, Text, Pair, IntWritable>
{
	private Pair sports_country = new Pair();
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
			sports_country.set(word[2].toLowerCase(),word[1].toLowerCase());
			context.write(sports_country, fans);
			}catch(Exception e){}
			
			
		}
	}

}
