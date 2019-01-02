package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Smapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	private Text outkey = new Text();
	private DoubleWritable outval = new DoubleWritable();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		
		String word[] = line.split(",");
		
		if(word!=null && word.length == 3)
		{
			try{
			outkey.set(word[0].toLowerCase());	//company name
			outval.set(Double.valueOf(word[2]));	//stock price
			context.write(outkey, outval);
			}catch(Exception e){}
			
			
		}
	}

}
