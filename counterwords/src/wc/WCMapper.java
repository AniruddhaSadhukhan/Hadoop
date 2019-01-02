package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper <LongWritable,Text,Text,IntWritable>{
	private Text outkey=new Text();
	private IntWritable outval = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line= value.toString();
		StringTokenizer st = new StringTokenizer(line);
		while(st.hasMoreTokens())
		{
			String word=st.nextToken();
			 outkey.set(word);
		
			context.write(outkey,outval);
		}
		
	}
	

}
