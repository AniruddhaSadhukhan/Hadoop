package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Smapper1 extends Mapper<LongWritable, Text, Text, Text>
{
	private Text sports = new Text();
	private Text country = new Text();
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
			country.set(word[1].toLowerCase());
			
			context.write(sports,country);
			}catch(Exception e){}
			
			
		}
	}

}
