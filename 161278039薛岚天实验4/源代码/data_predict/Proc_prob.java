package data_predict;

//import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
//import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
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
//import org.apache.hadoop.util.StringUtils;


public class Proc_prob {
	/*public static int kp=0;
	public static int kn=0;
	public static int kneu=0;
	public static int mkneu=0;
	public static int mkp=0;
	public static int mkn=0;
	public static int kt=0;
	public static int mkt=0;
	*/
	//map
	public static class probMap extends Mapper<Object, Text, Text,Text> { 
		private Text result = new Text();
		private Text word = new Text(); 
		//private List<String> patternsToSkip = new ArrayList<String>();
		
		/*public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			int i=0;
			for(URI patternsURI : patternsURIs){
				Path patternsPath = new Path(patternsURI.getPath());
				String fileName = patternsPath.getName().toString();
				//parseSkipFile(patternsFileName);
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
		}*/
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lines;
			lines=line.split("\t");
			if(line.contains("!")) {//the number of files the word appear in
				/*word.set(lines[0].split("!")[0]);
				result.set(Integer.parseInt(lines[1]));
				context.write(word,result);*/
			}
			else {
				word.set(lines[0].split("@")[0]);
				result.set(lines[1]+"@"+lines[0].split("@")[1]);
				System.out.print(word.toString()+" ");
				System.out.println(result.toString());
				context.write(word,result);
			}
		}
	}
	
	
	public static class probReduce extends Reducer< Text,Text, Text, Text> { 
		//private double freqP,freqN,freqNeu;
		
		/*public void setup(Context context) throws IOException,InterruptedException{
			freqP=(double)mkp/mkt;
			freqN=(double)mkn/mkt;
			freqNeu=(double)mkneu/mkt;
		}*/
		
		private Text word1 = new Text();
  		private String prob ;
  		String temp = new String();
  		String[] temps;
  		@Override
  		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
  			int sump = 0,sumn=0,sumneu=0;
  			int sum=0;
  			double probp,probn,probneu;
  			for (Text val : values) {
  				temp=val.toString();
  				temps=temp.split("@");
  				if(temps[1].contains("pos")) sump+=Integer.parseInt(temps[0]);
  				else if(temps[1].contains("neg")) sumn+=Integer.parseInt(temps[0]);
  				else sumneu+=Integer.parseInt(temps[0]);	
  			}
  			sum=sump+sumn+sumneu;
  			if(sum>10) {
  				probp=(double)sump/sum;
  	  			probn=(double)sumn/sum;
  	  			probneu=(double)sumneu/sum;
  	  			/*probp=(double)sump/sum/freqP;
  	  			probn=(double)sumn/sum/freqN;
  	  			probneu=(double)sumneu/sum/freqNeu;*/
  	  			prob=probp+";"+probn+";"+probneu;
  	  			word1.set(prob);
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
            System.out.println("previous output path removed!");
        } 
		Job job1 = new Job(conf1,"data_process1");
		job1.setJarByClass(Proc_prob.class); 
		
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(Text.class); 
		job1.setMapperClass(probMap.class); 
		job1.setReducerClass(probReduce.class); 
		job1.setInputFormatClass(TextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
	}*/
}
