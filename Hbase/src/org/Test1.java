package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Test1 {

	public static void creatTable(String tableName, String[] familys)
	throws Exception {
	Configuration conf = HBaseConfiguration.create();
	HBaseAdmin admin = new HBaseAdmin(conf);
	if (admin.tableExists(tableName)) {
	System.out.println("table already exists!");
	} else {
	HTableDescriptor tableDesc = new HTableDescriptor(tableName);
	for (int i = 0; i < familys.length; i++) {
	tableDesc.addFamily(new HColumnDescriptor(familys[i]));
	}
	admin.createTable(tableDesc);
	System.out.println("create table " + tableName + " ok.");
	}
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String tn = "student";
		String [] cf = {"personal","address"};
		creatTable(tn,cf);

	}

}
