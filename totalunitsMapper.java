package television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class totalunits extends Mapper<LongWritable, Text, Text, Tesxt> {
		
	Text Company;
	Text Product;
	
	@Override
	public void setup(Context context) {
		Company = new Text();
		Product = new Text();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("|");
		
		Company.set(lineArray[0]);
		Product.set(lineArray[1]);
		
		context.write(Company, Product);
	}
}