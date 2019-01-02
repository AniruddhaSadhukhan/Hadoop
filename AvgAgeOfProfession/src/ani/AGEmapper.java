package ani;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AGEmapper extends Mapper<LongWritable, Text, Text, TupleWritable>
{
	private Text prof = new Text();
	private TupleWritable age = new TupleWritable();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString().trim();
		
		String word[] = line.split(",");
		
		if(word!=null && word.length == 5)
		{
			try{
			prof.set(word[4].toLowerCase());
			age.set(Double.valueOf(word[3]),1);
			context.write(prof, age);
			}catch(Exception e){}
			
			
		}
	}

}
