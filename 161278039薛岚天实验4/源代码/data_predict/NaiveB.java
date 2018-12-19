package data_predict;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveB {
	public static class NaiveMap extends Mapper<Object, Text, Text,Text> { 
		private Text word = new Text(); 
		private Text result = new Text();
			
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
			String line = value.toString();
			String[] lines = line.split("\t");
			word.set(lines[0]);
			result.set(lines[1]);
			context.write(word, result);
		} 
	}
	
	public static class NaiveBReduce extends Reducer<Text,Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double dp=0,dn=0,dneu=0;
			String temp=null;
			for (Text val : values) {
				temp=val.toString();
				String[]temps=temp.split("/");
				if(temps[1].equals("pos")) dp+=Double.parseDouble(temps[0]);
				else if(temps[1].equals("neg")) dn+=Double.parseDouble(temps[0]);
				else dneu+=Double.parseDouble(temps[0]);
			}
			if(dp>dn) {
				if(dp>dneu) context.write(key,new Text("pos"));
				else context.write(key, new Text("neu"));
			}else {
				if(dn>dneu) context.write(key, new Text("neg"));
				else context.write(key, new Text("neu"));
			}
			
		} 
	}

}
