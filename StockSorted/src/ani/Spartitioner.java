package ani;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class Spartitioner extends Partitioner<Pair,DoubleWritable> //takes mapper output type
{

	@Override
	public int getPartition(Pair key, DoubleWritable val, int no_of_partitioner) 
	{
		return key.getName().hashCode() % no_of_partitioner;
	}
	
}
