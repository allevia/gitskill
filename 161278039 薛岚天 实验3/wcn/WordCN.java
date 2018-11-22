package wcn;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException; 
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

import wcn.Sorting.SortMap;
import wcn.Sorting.SortReduce;

public class WordCN {
	public static int k;
	//map
	public static class WordCNMap extends Mapper<Object, Text, Text,IntWritable> { 
		private IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		private List<String> patternsToSkip = new ArrayList<String>();
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //stop words text
			for(URI patternsURI : patternsURIs){
				Path patternsPath = new Path(patternsURI.getPath());
				String patternsFileName = patternsPath.getName().toString();
				parseSkipFile(patternsFileName);
			}
		}
		
		//skip
		private void parseSkipFile(String fileName){
			try{
				BufferedReader fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while((pattern = fis.readLine())!=null){
					patternsToSkip.add(pattern);
				}
			}catch (IOException ioe){
				System.err.println("Caught exception while parsing the cached file"+StringUtils.stringifyException(ioe));
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
			String line = value.toString();
			String[] lines = line.split("\t");
			String theline;
			if(lines.length==6)
				theline = lines[4];
			else if(lines.length==7)
				theline=lines[4]+","+lines[5];
			else return;
			for(String pattern : patternsToSkip){
				theline=theline.replaceAll("\u3000", "");
				theline = theline.replace(pattern,"");
			}
		
			Result words = ToAnalysis.parse(theline);
			
			for(int i=0;i<words.size();i++) {
				String wor=words.get(i).toString();
				if(!wor.contains("/")) continue; //skip the signs
				if (wor.endsWith("en")||wor.endsWith("m") )continue; //skip English and numbers
				word.set(wor);
				context.write(word,one);
			}	 
		}
	}
	
	public static class WordCNReduce extends Reducer< Text,IntWritable, Text, IntWritable> { 
		private IntWritable result = new IntWritable();
		
		public void setup(Context context) throws IOException,InterruptedException{
			k = Integer.parseInt(context.getConfiguration().get("k"));
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
			int sum = 0; 
			for (IntWritable val : values) { 
				sum += val.get(); 
			}
			if(sum>k){
				result.set(sum);
				context.write(key,result); //output <word,times>
			}
		} 
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration(); //word count
		conf1.setInt("k", Integer.parseInt(args[3]));
		
		Job job1 = new Job(conf1,"word CN");
		job1.setJarByClass(WordCN.class); 
		job1.addCacheFile(new Path(args[4]).toUri());
		
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(IntWritable.class); 
		job1.setMapperClass(WordCNMap.class); 
		job1.setReducerClass(WordCNReduce.class); 
		job1.setInputFormatClass(TextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration(); //sort
		
		Job job2 = new Job(conf2,"sorting");
		job2.setJarByClass(Sorting.class); 
		
		job2.setMapOutputKeyClass(IntWritable.class); 
		job2.setMapOutputValueClass(Text.class); 
		job2.setMapperClass(SortMap.class); 
		job2.setReducerClass(SortReduce.class); 
		job2.setInputFormatClass(TextInputFormat.class); 
		job2.setOutputFormatClass(TextOutputFormat.class); 
		job2.setOutputKeyClass(Text.class); 
		job2.setOutputValueClass(IntWritable.class); 
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
	}
}
