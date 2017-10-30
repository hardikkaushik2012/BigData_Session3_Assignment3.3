package television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class totalunitsReducer extends Reducer<Text, Text, Text, IntWritable>
{	
	IntWritable totalunits;
	
	@Override
	public void setup(Context context) {
		totalunits = new IntWritable();
	}
	
	@Override
	public void reduce(Text key, Text values,Context context) throws IOException, InterruptedException
	{
		int units = 0;
		
		for (Text value : values)
			{
				units=units +1;
			}
				
		totalunits.set(units);
		context.write(key, totalunits);
	}
}