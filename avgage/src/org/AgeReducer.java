package org;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AgeReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable>{

	private DoubleWritable outval= new DoubleWritable();
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
			throws IOException, InterruptedException {
		
		double total = 0;
		int count = 0;
		for(DoubleWritable age: values){
			
			total= total +age.get();
			count= count +1;
			
		}
		double avg= total/count;
		outval.set(avg);
		context.write(key, outval);
		
	}

	
}
