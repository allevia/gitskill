package data_predict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FSDataOutputStream; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.StringUtils;


import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Tf_idf_train {
	//public static double[] times;
	public static Map<String,String> mapf= new HashMap<String,String>();
	public static String[] uwords= {"跌/v","涨/v","弱势/n","产业/n","回应/v","仓/ng","审核/v","焦点/n","低估/v","强/a"};
	//map
	public static class trainMap extends Mapper<Object, Text, Text,Text> { 
		private Text result = new Text();
		private Text word = new Text(); 
		private IntWritable one =new IntWritable(1);
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
			String filepath = fileSplit.getPath().toString(); //get filename of the split
			String fileName=fileSplit.getPath().getName().toString();
			String sign;
			int i;
			System.out.println(fileName);
			if(filepath.contains("neg")) sign="neg";
			else if(filepath.contains("pos")) sign="pos";
			else sign="neu";
			
			String line = value.toString();
			System.out.println(line);
			for(String pattern : patternsToSkip){
				line=line.replaceAll("\u3000", "");
				line =line.replace(pattern,"");
			}
			//System.out.println("!!!");
			Result words = ToAnalysis.parse(line);
			//System.out.println("~~~");
			for(i=0;i<words.size();i++) {
				String wor=words.get(i).toString();
				if(!wor.contains("/")) continue; //skip the signs
				if (wor.endsWith("en")||wor.endsWith("m")||wor.endsWith("/w") )continue; //skip English and numbers and symbols
				if(mapf.containsKey(wor)) {
					word.set(fileName+"@"+sign);
					result.set(1+"@"+wor);
					//System.out.println(result.toString());
					context.write(word,result);
				}
				word.set(fileName+"@"+sign);
				result.set(1+"@"+sign);
				//System.out.println(result.toString());
				context.write(word, result); //calculate the total number of words in file
			}
		}		
	}
	
	/*public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
  		@Override
  		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
  			String term = new String();
  			term = key.toString().split("@")[0]; // <term#docid>=>term
  			return super.getPartition(new Text(term), value, numReduceTasks);
  		}
  	}*/
	
	public static class trainReduce extends Reducer< Text,Text, NullWritable, Text> { 
		private Text result = new Text();
		private Text text1=new Text();
		private NullWritable ob;
		private double[] times=new double[uwords.length];
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			int sum = 0,i,flag=0; 
			String temp;
			String[] temps;
			String sign1="",outr="";
			for(i=0;i<uwords.length;i++) times[i]=0;
			for (Text val : values) { 
				temp=val.toString();
				temps=temp.split("@");
				if(temps[1].equals("pos")||temps[1].equals("neg")||temps[1].equals("neu")) {
					sum+=Integer.parseInt(temps[0]);//the total number in a file
					sign1=temps[1];
				}
				if(mapf.containsKey(temps[1])) {
					for(i=0;i<uwords.length;i++) {
						if(temps[1].equals(uwords[i])) times[i]+=Double.parseDouble(temps[0]);
					}
				}
			}
			for(i=0;i<uwords.length;i++)
				if(times[i]!=0) flag=1;
			if(flag==1) {
				for(i=0;i<uwords.length;i++) times[i]=times[i]/sum*Double.parseDouble(mapf.get(uwords[i]));
				for(i=0;i<uwords.length;i++) outr=outr+times[i]+" ";
				outr=outr+sign1;
				System.out.println(outr);
				result.set(outr);
				context.write(ob,result); 
			}
		} 
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration();
		Path mypath = new Path(args[4]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
            System.out.println("previous output path of tf-idf(train) removed!");
        } 
        
        Configuration conf0 = new Configuration(); 
		Path mypath0 = new Path(args[1]); 
  		FileSystem hdfs0 = mypath.getFileSystem(conf0);  
        if (hdfs0.isDirectory(mypath0)) {  
            hdfs0.delete(mypath0, true);  
            System.out.println("previous output path of ITF_Train removed!");
        } 
        
		Job job0 = new Job(conf0,"data_process2");
		job0.setJarByClass(ITF.class); 
		job0.addCacheFile(new Path(args[2]).toUri());
		job0.setMapOutputKeyClass(Text.class);
  		job0.setMapOutputValueClass(IntWritable.class);
		job0.setOutputKeyClass(Text.class); 
		job0.setOutputValueClass(Text.class); 
		job0.setMapperClass(ITF.ITFMap.class); 
		job0.setReducerClass(ITF.ITFReduce.class); 
		job0.setInputFormatClass(TextInputFormat.class); 
		job0.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileOutputFormat.setOutputPath(job0, new Path(args[1]));
		job0.waitForCompletion(true);
        
        String filepath=args[1]+"/part-r-00000";
  		Path path1=new Path(filepath);
  		FileSystem fs=path1.getFileSystem(conf1);
  		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(path1)));
  		String line;
  		while((line= reader.readLine())!=null) {
  			//System.out.println(line);
  			for(int i=0;i<uwords.length; i++) {
  				if(line.split("\t")[0].equals(uwords[i])) mapf.put(line.split("\t")[0],line.split("\t")[1]);
  			}
  		}
  		System.out.println("IDFs get!");
        
		Job job1 = new Job(conf1,"tf_idf_train");
		job1.setJarByClass(Tf_idf_train.class); 
		job1.addCacheFile(new Path(args[5]).toUri());
		job1.setMapperClass(trainMap.class); 
		job1.setMapOutputKeyClass(Text.class); 
		job1.setMapOutputValueClass(Text.class); 
		job1.setReducerClass(trainReduce.class); 
		job1.setOutputKeyClass(NullWritable.class); 
		job1.setOutputValueClass(Text.class); 
		job1.setInputFormatClass(TextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class); 
		FileInputFormat.addInputPath(job1, new Path(args[3]));
		FileOutputFormat.setOutputPath(job1, new Path(args[4]));
		if((job1.waitForCompletion(true)?true:false)){
  			System.out.println("get the tf-idf vectors for train data!");
  		}
	}
}


