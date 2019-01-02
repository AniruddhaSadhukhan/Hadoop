package ani;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DCmapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private Text outkey = new Text();
	private IntWritable outval = new IntWritable(1);
	private ArrayList<String> cwlist = new ArrayList<String> ();
	
	
	
	//before map starts ,setup finishes
	@Override
	protected void setup(Context context) throws IOException,InterruptedException 
	{
		Path path[] = context.getLocalCacheFiles();
		if (path != null && path.length > 0)
		{
			for(Path p : path)
			{
				String strpath = p.toString();
				FileReader file = new FileReader(strpath);
				BufferedReader breader = new BufferedReader(file);
				
				while(true)
				{
					String word = breader.readLine();
					if (word == null) break;
					cwlist.add(word);
				}
				
				breader.close();
				file.close();
				
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(line); //memory efficient
		
		while(tokenizer.hasMoreTokens())
		{
			String word = tokenizer.nextToken();
			word = word.replaceAll("\\W+", ""); //replaces all nonword characters eg.(,),, etc
			
			if(cwlist.contains(word.toLowerCase())) continue;
			
			outkey.set(word);
			
			context.write(outkey, outval);
		}
		
	}
	
	
	
}
