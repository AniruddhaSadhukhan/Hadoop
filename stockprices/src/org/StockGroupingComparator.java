package org;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StockGroupingComparator extends WritableComparator{

	public StockGroupingComparator(){
		
		super(SPkey.class,true);
		
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		SPkey key1=(SPkey)a;
		SPkey key2=(SPkey)b;
		String s1=key1.getStock();
		String s2=key2.getStock();
		int c=s1.compareTo(s2);
		return c;
	}
	
}
