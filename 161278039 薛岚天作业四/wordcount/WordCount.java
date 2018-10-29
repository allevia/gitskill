package wc3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException; 
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer; 

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount {
	public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> { 
		private final static IntWritable one = new IntWritable(1); 
		private Text word = new Text(); 
		private boolean caseSensitive;
		private List<String> patternsToSkip = new ArrayList<String>();
		
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			//check whether the system variable exists and whether its value is true
			caseSensitive = Boolean.getBoolean("wordcount.case.sensitive");
			
			if(conf.getBoolean("wordcount.skip.patterns",true)){
				URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
				for(URI patternsURI : patternsURIs){
				    Path patternsPath = new Path(patternsURI.getPath());
				    String patternsFileName = patternsPath.getName().toString();
				    parseSkipFile(patternsFileName);
				}
			}
		}
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
			String line = (caseSensitive)?value.toString():value.toString().toLowerCase();
			for(String pattern : patternsToSkip){
				line=line.replaceAll("\u3000", "");//eliminate the blank at the beginning
				line = line.replace(pattern,"");
			}
			
			StringTokenizer token = new StringTokenizer(line); //split
			while (token.hasMoreTokens()) {
				word.set(token.nextToken()); 
				context.write(word, one); //<key,value>
			} 
		} 
	}
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> { 
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
			int sum = 0; 
			for (IntWritable val : values) { 
				sum += val.get(); 
			} 
			result.set(sum);
			context.write(key, result); //output <key,value>
		} 
	}
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration(); 
		//get the value of wordcount.case.sensitive
		System.getProperty("wordcount.case.sensitive");
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(!(otherArgs.length!=2 || otherArgs.length!=4)){
			System.err.println("Usage:wordcount<in><out>");
			System.exit(2);
		}
		Job job = new Job(conf,"word count");
		job.setJarByClass(WordCount.class); 
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class); 
		job.setMapperClass(WordCountMap.class); 
		job.setCombinerClass(WordCountReduce.class);
		job.setReducerClass(WordCountReduce.class); 
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class); 
		List<String> myArgs = new ArrayList<String>();
		job.getConfiguration().setBoolean("wordcount.skip.patterns", false);
		//job.getConfiguration().setBoolean("wordcount.case.sensitive", false);
		for(int i=0;i < otherArgs.length;++i){
			if("-skip".equals(otherArgs[i])){
				job.addCacheFile(new Path(otherArgs[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			}else{
				myArgs.add(otherArgs[i]);
			}
		}
		
		FileInputFormat.addInputPath(job, new Path(myArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(myArgs.get(1)));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}