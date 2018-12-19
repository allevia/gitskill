package data_predict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException; 
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FSDataOutputStream; 
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Data_proc {
	public static int kp=0;
	public static int kn=0;
	public static int kneu=0;
	public static int mkneu=0;
	public static int mkp=0;
	public static int mkn=0;
	public static int kt=0;
	public static int mkt=0;
	//map
	public static class dataMap extends Mapper<Object, Text, Text,IntWritable> { 
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
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().toString(); //get filename of the split
			String sign;
			int i;
			System.out.println(fileName);
			if(fileName.contains("neg")) {
				sign="neg";
				kn++;
			}
			else if(fileName.contains("pos")) {
				sign="pos";
				kp++;
			}
			else {
				sign="neu";
				kneu++;
			}
			kt++;
			String line = value.toString();
			System.out.println(line);
			for(String pattern : patternsToSkip){
				line=line.replaceAll("\u3000", "");
				line =line.replace(pattern,"");
			}
		
			Result words = ToAnalysis.parse(line);
			Set<String> set=new HashSet<String>();
			
			for(i=0;i<words.size();i++) {
				String wor=words.get(i).toString();
				if(!wor.contains("/")) continue; //skip the signs
				if (wor.endsWith("en")||wor.endsWith("m")||wor.endsWith("/w") )continue; //skip English and numbers and symbols
				set.add(wor);
				word.set(wor+'@'+sign);
				if(sign.equals("pos")) mkp++;
				else if(sign.equals("neg")) mkn++;
				else mkneu++;
				mkt++;
				context.write(word,one);
			}
			
			for(i=0;i<set.toArray().length;i++) {
				word.set("!"+set.toArray()[i].toString());
				context.write(word, one);
			}
			
		}
	}
	
	public static class dataReduce extends Reducer< Text,IntWritable, Text, IntWritable> { 
		private IntWritable result = new IntWritable();
		
		/*public void setup(Context context) throws IOException,InterruptedException{
			k = Integer.parseInt(context.getConfiguration().get("k"));
		}*/
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
			int sum = 0; 
			for (IntWritable val : values) { 
				sum += val.get(); 
			}
				result.set(sum);
				context.write(key,result); //output <word,times>
		} 
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration(); //word count
		//conf1.setInt("k", Integer.parseInt(args[3]));
		Path mypath = new Path(args[1]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
            System.out.println("previous output path removed!");
        } 
		Job job1 = new Job(conf1,"data_process");
		job1.setJarByClass(Data_proc.class); 
		job1.addCacheFile(new Path(args[2]).toUri());
		
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(IntWritable.class); 
		job1.setMapperClass(dataMap.class); 
		job1.setReducerClass(dataReduce.class); 
		job1.setInputFormatClass(TextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		String num=kp+";"+kn+";"+kneu+";"+kt+"\n"+mkp+";"+mkn+";"+mkneu+";"+mkt+"\n";
		System.out.println(num);
		String outfile="hdfs://localhost:9000/user/user/freq";
		Path path1=new Path(outfile);
		FileSystem fs=path1.getFileSystem(conf1);
		FSDataOutputStream output1=null;
		try {
			output1=fs.create(path1);
			output1.write(num.getBytes(),0,num.length());
			output1.flush();
		}catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				output1.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
}
