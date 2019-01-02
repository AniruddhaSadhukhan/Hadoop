package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Pmapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
	private NullWritable outkey = NullWritable.get();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		
		String word[] = line.split(",");
		
		if(word!=null && word.length == 5)
		{
			if(word[4].equalsIgnoreCase("pilot"))
			{
				context.write(outkey,value);
			}
		}
	}

}
