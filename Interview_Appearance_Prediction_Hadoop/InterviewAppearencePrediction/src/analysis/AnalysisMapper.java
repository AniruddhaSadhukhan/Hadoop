package analysis;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
	private Text outval = new Text();
	private NullWritable outkey = NullWritable.get();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
	throws IOException, InterruptedException 
	{
			String line = value.toString().trim();
			String word[] = line.split("\t");
			String observed  = word[1].split(" ")[2].toLowerCase().trim();
			String predicted = word[2].split(" ")[2].toLowerCase().trim();
			
			outval.set(observed+" "+predicted);
			
			
			context.write(outkey, outval);
			
	}
}
