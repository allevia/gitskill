package data_predict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Search_Data{
  public static Map<String,String> mapf= new HashMap<String,String>();
	 
  public static class SDMapper extends Mapper<Object, Text, Text, IntWritable> {
	  private List<String> stopwords = new ArrayList<String>();
	  private URI[] localFiles;
	  private Text word = new Text(); 
	  private IntWritable one = new IntWritable(1);
	  
	  public void setup(Context context) throws IOException, InterruptedException {
		  Configuration conf = context.getConfiguration();
		  localFiles = Job.getInstance(conf).getCacheFiles();// get the stopwords
		  for (URI localFile : localFiles) {
			  Path patternsPath = new Path(localFile.getPath());
			  String line=null;
			  BufferedReader br =new BufferedReader(new FileReader(patternsPath.getName().toString()));
			  while ((line = br.readLine()) != null) {
				  stopwords.add(line);
			  }
		  }
	  }

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] lines = line.split("\t");
		  String theline="";
		  String thecode="";
		  if(lines.length==6) {
			  thecode=lines[0];
			  theline = lines[4];
		  }	
		  else if(lines.length==7) {
			  thecode=lines[0];
			  theline=lines[4]+","+lines[5];
			}	
		  else return;
		  
		  for(String pattern : stopwords){
			  theline = theline.replace(pattern,""); //skip the stop-word
		  }
		  Result words = ToAnalysis.parse(theline);
		  
		  for(int i=0;i<words.size();i++) {
			  String wor=words.get(i).toString();
			  if(!wor.contains("/")) continue; // skip the signs
			  if (wor.endsWith("en") || wor.endsWith("m")||wor.endsWith("/w"))continue; //skip English and number
			  word.set(wor+"@"+thecode);
			  //System.out.println(word.toString());
			  context.write(word, one); 
		  }
	  } 
  }


  	public static class SDReducer extends Reducer<Text,IntWritable, Text, Text> {
  		private Text word1 = new Text();
  		private Text word2 = new Text();
  		String temp = new String();
  		@Override
  		public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
  			int sum = 0;
  			double weightp=0,weightn=0,weightneu=0;
  			word1.set(key.toString().split("@")[1]);
  			//System.out.println(key.toString());
  			temp = key.toString().split("@")[0];
  			for (IntWritable val : values) {
  				sum += val.get();
  			}
  			if(mapf.containsKey(temp)) {
  				weightp=sum*Double.parseDouble(mapf.get(temp).split(";")[0]);
  				weightn=sum*Double.parseDouble(mapf.get(temp).split(";")[1]);
  				weightneu=sum*Double.parseDouble(mapf.get(temp).split(";")[2]);
  				word2.set(weightp+"/pos");
  				context.write(word1,word2);
  				word2.set(weightn+"/neg");
  				context.write(word1,word2);
  				word2.set(weightneu+"/neu");
  				context.write(word1,word2);
  			}
  		}	
  			

  	}

  	public static void main(String[] args) throws Exception {
  		Configuration conf1 = new Configuration();
  		Path mypath = new Path(args[1]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
            System.out.println("the previous output of SD removed!");
        } 
        Path mypath1 = new Path(args[3]); 
  		FileSystem hdfs1 = mypath1.getFileSystem(conf1);  
        if (hdfs1.isDirectory(mypath1)) {  
            hdfs1.delete(mypath1, true);  
            System.out.println("the previous output of predict removed");
        }
        
        Configuration conf0 = new Configuration(); 
		Path mypath0 = new Path(args[5]); 
  		FileSystem hdfs0 = mypath0.getFileSystem(conf0);  
        if (hdfs0.isDirectory(mypath0)) {  
            hdfs0.delete(mypath0, true);  
            System.out.println("previous output path of prob removed!");
        } 
		Job job0 = new Job(conf0,"data_process1");
		job0.setJarByClass(Proc_prob.class); 
		
		job0.setOutputKeyClass(Text.class); 
		job0.setOutputValueClass(Text.class); 
		job0.setMapperClass(Proc_prob.probMap.class); 
		job0.setReducerClass(Proc_prob.probReduce.class); 
		job0.setInputFormatClass(TextInputFormat.class); 
		job0.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job0, new Path(args[4]));
		FileOutputFormat.setOutputPath(job0, new Path(args[5]));
		if((job0.waitForCompletion(true)?true:false)){
  			System.out.println("primal stage finished!");
  		}
     
  		String filepath=args[5]+"/part-r-00000";
  		Path path1=new Path(filepath);
  		FileSystem fs=path1.getFileSystem(conf1);
  		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(path1)));
  		String line;
  		while((line= reader.readLine())!=null) {
  			System.out.println(line);
  			mapf.put(line.split("\t")[0],line.split("\t")[1]);
  		}
  		System.out.println("probabilities get!");
  		
  		Job job1 = new Job(conf1, "search data process");
  		job1.addCacheFile(new Path(args[2]).toUri());
  		job1.setJarByClass(Search_Data.class);
  		job1.setMapperClass(SDMapper.class);
  		job1.setMapOutputKeyClass(Text.class);
  		job1.setMapOutputValueClass(IntWritable.class);
  		job1.setReducerClass(SDReducer.class);
  		job1.setInputFormatClass(TextInputFormat.class);
  		job1.setOutputFormatClass(TextOutputFormat.class); 
  		job1.setOutputKeyClass(Text.class);
  		job1.setOutputValueClass(Text.class);
  		FileInputFormat.addInputPath(job1, new Path(args[0]));
  		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
  		if((job1.waitForCompletion(true)?true:false)){
  			System.out.println("stage1 finished!");
  		}
  		
  		Configuration conf2 = new Configuration();
  		Job job2 = new Job(conf2, "data predict process");
  		job2.addCacheFile(new Path(args[2]).toUri());
  		job2.setJarByClass(NaiveB.class);
  		job2.setMapperClass(NaiveB.NaiveMap.class);
  		job2.setReducerClass(NaiveB.NaiveBReduce.class);
  		job2.setInputFormatClass(TextInputFormat.class);
  		job2.setOutputFormatClass(TextOutputFormat.class); 
  		job2.setOutputKeyClass(Text.class);
  		job2.setOutputValueClass(Text.class);
  		FileInputFormat.addInputPath(job2, new Path(args[1]));
  		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
  		if((job2.waitForCompletion(true)?true:false)){
  			System.out.println("predict using NaiveBayes finished!");
  		}
  		
  		
  	}
}
