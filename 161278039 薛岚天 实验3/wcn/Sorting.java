package wcn;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sorting {
	//map
	public static class SortMap extends Mapper<Object, Text, IntWritable, Text> { 
		private Text word = new Text(); 
		private IntWritable result = new IntWritable();
			
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
			String line = value.toString();
			String[] lines = line.split("\t");
			word.set(lines[0]);
			result.set(Integer.parseInt(lines[1])*(-1));
			context.write(result, word);
		} 
	}
	
	//reduce
	public static class SortReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
		private IntWritable number = new IntWritable();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			for (Text val : values) {
				number.set(key.get()*(-1));
				context.write(val, number);
			}
		} 
	}
}

