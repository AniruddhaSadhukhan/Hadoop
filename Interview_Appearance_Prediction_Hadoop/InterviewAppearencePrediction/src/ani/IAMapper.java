package ani;

import java.io.IOException;
//import java.util.Calendar;
//import java.util.regex.*;
//import java.text.SimpleDateFormat;
//import java.util.Locale;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class IAMapper extends Mapper<LongWritable,Text,Text,TupleWritable>
{
	private Text outkey = new Text();
	private TupleWritable outval = new TupleWritable();
	private int i;

	@Override
	protected void map(LongWritable key, Text value,Context context)
	throws IOException, InterruptedException 
	{
		if(key.get()!=0)
		{
			String line = value.toString().trim();
			String word[] = line.split(",");

			if(word!=null && word.length >22 && word[7]!=null)
			{
				try
				{
					for(i=1;i<20;i++)
					{
						if(i>0 && i<5)
						{
							outkey.set("x"+i+"_"+word[i].toLowerCase().replaceAll("[^A-Za-z]+", ""));
						}
						else if(i==5)
						{
							if(Character.isDigit(word[5].charAt(0)))
								outkey.set("x5_unknown");
							else{
								word[5] = word[5].toLowerCase().replaceAll("[^A-Za-z]+","");
								if(StringUtils.getLevenshteinDistance(word[5] ,"biosimilars")<4)
									outkey.set("x5_biosimilars");
								else if(StringUtils.getLevenshteinDistance(word[5] ,"lendingliablities")<11)
									outkey.set("x5_lendingliablities");
								else outkey.set("x5_"+word[5]);	
							}
						}
						else if(i==6)
						{
							word[6] = word[6].toLowerCase().replaceAll("[^A-Za-z]+","");
							if(StringUtils.getLevenshteinDistance(word[6] ,"scheduledwalkin")<3)
								outkey.set("x6_scheduledwalkin");
							else outkey.set("x6_"+word[6]);	
						}
						else if(i==7)
						{
							if(word[8].equalsIgnoreCase("male"))
								outkey.set("x7_male");
							else outkey.set("x7_female");
						}
						else if(i==8)
						{
							if(word[9].equalsIgnoreCase(word[10]))
								outkey.set("x8_yes");
							else outkey.set("x8_no");
						}
						else if(i==9)
						{
							if(word[9].equalsIgnoreCase(word[11]))
								outkey.set("x9_yes");
							else outkey.set("x9_no");
						}
						else if(i==10)
						{
							if(word[12].equalsIgnoreCase(word[10]))
								outkey.set("x10_yes");
							else outkey.set("x10_no");
						}
						else if(i>10 && i<14)
						{
							if(word[i+2].equalsIgnoreCase("yes"))
								outkey.set("x"+i+"_"+"yes");
							else if(word[i+2].equalsIgnoreCase("no"))
								outkey.set("x"+i+"_"+"no");
							else if(word[i+2].equalsIgnoreCase("NA"))
								outkey.set("x"+i+"_"+"na");
							else 
								outkey.set("x"+i+"_"+"uncertain");
						}
						else if(i>13 && i<17)
						{
							if(word[i+3].equalsIgnoreCase("yes"))
								outkey.set("x"+i+"_"+"yes");
							else if(word[i+3].equalsIgnoreCase("no"))
								outkey.set("x"+i+"_"+"no");
							else if(word[i+3].equalsIgnoreCase("NA"))
								outkey.set("x"+i+"_"+"na");
							else 
								outkey.set("x"+i+"_"+"uncertain");
						}
						else if(i==17)
						{
							if(word[20].equalsIgnoreCase("no"))
								outkey.set("x17_"+"no");
							else if(word[20].equalsIgnoreCase("UNCERTAIN"))
								outkey.set("x17_"+"uncertain");
							else if(word[20].equalsIgnoreCase("NA"))
								outkey.set("x17_"+"na");
							else 
								outkey.set("x17_"+"yes");
						}
						else if(i==18)
						{
							if(word[22].equalsIgnoreCase("Single"))
								outkey.set("x18_s");
							else outkey.set("x18_m");
						}
						else if(i==19)
						{
							outkey.set("y");
						}



						if(word[21].equalsIgnoreCase("yes"))
							outval.set(1, 0);
						else outval.set(0, 1);

						context.write(outkey, outval);
					}

				}catch(Exception e){}


			}


		}

	}

}
