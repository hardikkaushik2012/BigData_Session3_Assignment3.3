package television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class totalunitsinstateforOnida extends Mapper<LongWritable, Text, Text, IntWritable> {
		
	Text Company;
	Text State;
	IntWritable count=0;
	
	@Override
	public void setup(Context context) {
		State = new Text();
		count = new IntWritable();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("|");
		
		Company.set(lineArray[0]);
		State.set(lineArray[3]);
		
		if(Company == "Onida" && State == "Uttar Pradesh")
		{
			count= 1;
			context.write(State, count);
		}
		else
		{
			if(Company == "Onida" && State == "Kerala")
				{
					count= 1;
					context.write(State, count);
				}
			
		}
	}
}