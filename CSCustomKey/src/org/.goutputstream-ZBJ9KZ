package org;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Spartitioner extends Partitioner<Text,Text> //takes mapper output type
{

	@Override
	public int getPartition(Text key, Text val, int no_of_partitioner) 
	{
		String sports = key.toString().toLowerCase();
		String country = val.toString().toLowerCase();
		
		if(sports.equals("cricket") && country.equals("india"))
			return 0;
		else if(sports.equals("cricket") && !country.equals("india"))
			return 1;
		else return 2;
	}
	
}
