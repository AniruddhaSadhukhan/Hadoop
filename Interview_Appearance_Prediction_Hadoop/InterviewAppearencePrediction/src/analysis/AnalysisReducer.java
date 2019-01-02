package analysis;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{
	private Text outval = new Text();
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Text> itr = values.iterator();
		int yy= 0,yn = 0, ny = 0,nn = 0;
		float accuracy;
		while(itr.hasNext())
		{
			outval = itr.next();
			String observed  = outval.toString().split(" ")[0].trim();
			String predicted = outval.toString().split(" ")[1].trim();
			
			if(observed.equals("yes") && predicted.equals("yes"))	yy=yy+1;
			else if (observed.equals("yes") && predicted.equals("no"))	yn=yn+1;
			else if (observed.equals("no") && predicted.equals("yes"))	ny=ny+1;
			else if (observed.equals("no") && predicted.equals("no")) nn=nn+1;
			
		}
		accuracy = ((float)(yy+nn)/(yy+yn+ny+nn)*100);
		String out = 	"Observed yes , Predicted yes : "+yy+
						"\nObserved yes , Predicted no  : "+yn+
						"\nObserved no  , Predicted yes : "+ny+
						"\nObserved no  , Predicted no  : "+nn+
						"\n\nAccuracy : "+accuracy+"%";
		
		outval.set(out);
		context.write(key, outval);
	}
	
}
