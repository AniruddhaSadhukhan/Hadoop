package org;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class StockPartitioner extends Partitioner<SPkey,DoubleWritable> {

	@Override
	public int getPartition(SPkey key, DoubleWritable value, int no) {
		
		int partno=key.getStock().hashCode() % no;
		return partno;
	}

}
