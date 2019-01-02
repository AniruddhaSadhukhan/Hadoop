package ani;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator 
{
	
	public GroupComparator() 
	{
		super(Pair.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) 
	{
			return ((Pair) a).getName().compareTo(((Pair) b).getName());
	}
	
}
