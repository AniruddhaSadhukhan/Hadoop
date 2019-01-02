package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TestMapper extends Mapper<LongWritable, Text, Text, Text>
{
	HashMap<String,String> map = new HashMap<String,String>();
	private Text outkey = new Text(),outval = new Text();


	@Override
	protected void setup(Context context)
	throws IOException, InterruptedException 
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
					String line = breader.readLine();
					if (line == null) break;
					String words[] = line.split("\t");
					map.put(words[0],words[1]+" "+words[2]);
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
		if(key.get()!=0)
		{
			String line = value.toString().trim();
			String word[] = line.split(","),attr ="",appear;
			int i;
			double Py,Pn, x[] = new double[2] ,ny[]= new double[2] ;
			if(word!=null && word.length >22 && word[7]!=null && word[7].startsWith("Candidate"))
			{
				ny[0] = Double.valueOf(map.get("y").split(" ")[0]);
				ny[1] = Double.valueOf(map.get("y").split(" ")[1]);

				//Initialise Py with P(y=yes) & Pn with P(y=no) 
				Py = ny[0]/(ny[0]+ny[1]);
				Pn = ny[1]/(ny[0]+ny[1]);

				for(i=2;i<19;i++)
				{
					if(i>0 && i<5)
					{
						attr="x"+i+"_"+word[i].toLowerCase().replaceAll("[^A-Za-z]+", "");
					}
					else if(i==5)
					{
						attr = word[5].toLowerCase().replaceAll("[^A-Za-z]+", "");
						if(StringUtils.getLevenshteinDistance(attr ,"biosimilars")<4)
							attr="x5_biosimilars";
						else if(StringUtils.getLevenshteinDistance(attr,"lendingliablities")<11)
							attr="x5_lendingliablities";
						else attr="x5_"+attr;	
					}
					else if(i==6)
					{
						attr = word[6].toLowerCase().replaceAll("[^A-Za-z]+", "");
						if(StringUtils.getLevenshteinDistance(attr ,"scheduledwalkin")<3)
							attr = "x6_scheduledwalkin";
						else attr = "x6_"+attr;							
					}
					else if(i==7)
					{
						if(word[8].equalsIgnoreCase("male"))
							attr = "x7_male";
						else attr = "x7_female";
					}
					else if(i==8)
					{
						if(word[9].equalsIgnoreCase(word[10]))
							attr = "x8_yes";
						else attr = "x8_no";
					}
					else if(i==9)
					{
						if(word[9].equalsIgnoreCase(word[11]))
							attr = "x9_yes";
						else attr = "x9_no";
					}
					else if(i==10)
					{
						if(word[12].equalsIgnoreCase(word[10]))
							attr = "x10_yes";
						else attr = "x10_no";
					}
					else if(i>10 && i<14)
					{
						if(word[i+2].equalsIgnoreCase("yes"))
							attr = "x"+i+"_"+"yes";
						else if(word[i+2].equalsIgnoreCase("no"))
							attr = "x"+i+"_"+"no";
						else if(word[i+2].equalsIgnoreCase("NA"))
							attr = "x"+i+"_"+"na";
						else 
							attr = "x"+i+"_"+"uncertain";
					}
					else if(i>13 && i<17)
					{
						if(word[i+3].equalsIgnoreCase("yes"))
							attr = "x"+i+"_"+"yes";
						else if(word[i+3].equalsIgnoreCase("no"))
							attr = "x"+i+"_"+"no";
						else if(word[i+3].equalsIgnoreCase("NA"))
							attr = "x"+i+"_"+"na";
						else 
							attr = "x"+i+"_"+"uncertain";
					}
					else if(i==17)
					{
						if(word[20].equalsIgnoreCase("no"))
							attr = "x17_"+"no";
						else if(word[20].equalsIgnoreCase("UNCERTAIN"))
							attr = "x17_"+"uncertain";
						else if(word[20].equalsIgnoreCase("NA"))
							attr = "x17_"+"na";
						else 
							attr = "x17_"+"yes";
					}
					else if(i==18)
					{
						if(word[22].equalsIgnoreCase("Single"))
							attr = "x18_s";
						else attr = "x18_m";
					}
					
					if (map.containsKey(attr))
					{
						x[0] = Double.valueOf(map.get(attr).split(" ")[0]);
						x[1] = Double.valueOf(map.get(attr).split(" ")[1]);

						//Updating Py = Py * P(x=attr | y = yes)
						//Updating Pn = Pn * P(x=attr | y = yes)
						Py = Py * x[0] / ny[0];
						Pn = Pn * x[1] / ny[1];
					}
				}


				//"yes = "+Py*100+"% \t no = "+Pn*100+"%"

				if (Py>=Pn)
					appear = "yes";
				else appear = "no ";

				if(appear.trim().equalsIgnoreCase(word[21].trim()))
					outkey.set("Output Matched    ");
				else outkey.set("Output Not Matched");
				outval.set("Observed : "+word[21].trim()+"\tPredicted : "+appear+" <--"+word[7].trim());

				context.write(outkey, outval);
			}
		}
	}
}
