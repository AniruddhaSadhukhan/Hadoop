package ani;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mmapper extends Mapper<Text, Text, Text, Text>
{
	private Pattern pattern;
	private String what = "^(\\d+\\.?\\d*),.*,(\\d+\\.?\\d*),$"; //?: 0 or max 1 , ^$ for anchoring, () for grouping, . for any char , * for any no of times
	private Text outval = new Text();
	
	
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException 
	{
		pattern = Pattern.compile(what);
	}


	@Override
	protected void map(Text key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		String record = value.toString().trim();
		
		Matcher m = pattern.matcher(record);
		
		if(m.matches())
		{
			outval.set(m.group(1)+","+m.group(2));
			context.write(key, outval);
		}
	}
	
}
