package data_predict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.StringUtils;


public class ITF {
	public static int kp=0;
	public static int kn=0;
	public static int kneu=0;
	public static int mkneu=0;
	public static int mkp=0;
	public static int mkn=0;
	public static int kt=0;
	public static int mkt=0;
	
	//map
	public static class ITFMap extends Mapper<Object, Text, Text,IntWritable> { 
		private IntWritable result = new IntWritable();
		private Text word = new Text(); 

		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			int i=0;
			for(URI patternsURI : patternsURIs){
				Path patternsPath = new Path(patternsURI.getPath());
				String fileName = patternsPath.getName().toString();
				try{
					BufferedReader fis = new BufferedReader(new FileReader(fileName));
					String pattern = null;
					for(i=0;i<2;i++){
						pattern=fis.readLine();
						String[] lines=pattern.split(";");
						if(i==0) {
							kp=Integer.parseInt(lines[0]);
							kn=Integer.parseInt(lines[1]);
							kneu=Integer.parseInt(lines[2]);
							kt=Integer.parseInt(lines[3]);
							//System.out.println(kt);
						}
						else {
							mkp=Integer.parseInt(lines[0]);
							mkn=Integer.parseInt(lines[1]);
							mkneu=Integer.parseInt(lines[2]);
							mkt=Integer.parseInt(lines[3]);
						}
					}
				}catch (IOException ioe){
					System.err.println("Caught exception while parsing the cached file"+StringUtils.stringifyException(ioe));
				}
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lines;
			lines=line.split("\t");
			if(line.contains("!")) {//the number of files the word appear in
				word.set(lines[0].split("!")[1]);
				result.set(Integer.parseInt(lines[1]));
				context.write(word,result);
			}
		}
	}
	
	
	public static class ITFReduce extends Reducer< Text,IntWritable, Text, Text> { 
		private Text word1 = new Text();
  		String temp = new String();
  		String[] temps;
  		@Override
  		public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
  			for (IntWritable val : values) {
  				int i=val.get();
  				double itf=(double)Math.log(kt/(i+1.0))/Math.log(10);
  				word1.set(itf+"");
  				context.write(key,word1);
  			}
  		}
	}
	
	/*public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration(); 
		Path mypath = new Path(args[1]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
            System.out.println("previous output path of ITF_Train removed!");
        } 
		Job job1 = new Job(conf1,"data_process2");
		job1.setJarByClass(ITF.class); 
		job1.addCacheFile(new Path(args[2]).toUri());
		job1.setMapOutputKeyClass(Text.class);
  		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(Text.class); 
		job1.setMapperClass(ITFMap.class); 
		job1.setReducerClass(ITFReduce.class); 
		job1.setInputFormatClass(TextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
	}*/
}
