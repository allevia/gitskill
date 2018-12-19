package data_predict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
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



public class Tf_idf_test {
	public static Map<String,String> mapf= new HashMap<String,String>();
	public static String[] uwords= {"跌/v","涨/v","弱势/n","产业/n","回应/v","仓/ng","审核/v","焦点/n","低估/v","强/a"};
	public static int tlength=0;
	public static double[] idf_test=new double [uwords.length];
	//map
	public static class testMap extends Mapper<Object, Text, Text,Text> { 
		private IntWritable one = new IntWritable(1);
		private Text result=new Text(); 
		private Text word = new Text(); 
		private List<String> stopwords = new ArrayList<String>();
		private URI[] localFiles;
		
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
			  int i,flag=0;
			  if(lines.length==6) {
				  thecode=lines[0];
				  theline = lines[4];
			  }else if(lines.length==7) {
				  thecode=lines[0];
				  theline=lines[4]+","+lines[5];
			  }else return;
			  
			  for(String pattern : stopwords){
				  theline = theline.replace(pattern,""); //skip the stop-word
			  }
			  //System.out.println(theline);
			  Result words = ToAnalysis.parse(theline);
			  
			  Set<String> set=new HashSet<String>();
			  for(i=0;i<words.size();i++) {
				  String wor=words.get(i).toString();
				  if(!wor.contains("/")) continue; // skip the signs
				  if (wor.endsWith("en") || wor.endsWith("m")||wor.endsWith("/w"))continue; //skip English and number
				  context.write(new Text(thecode), new Text(1+""));//the total number of words in one file
				  if(mapf.containsKey(wor)) {
					  set.add(wor);
					  word.set(thecode);
					  result.set(1+"@"+wor);
					  //System.out.println(word.toString());
					  context.write(word, result); 
				  }
			  }
			  for(i=0;i<set.toArray().length;i++) {
					word.set("!"+set.toArray()[i].toString());
					result.set(1+"");//calculate the file number it appears in
					context.write(word, result);
				}
			  tlength++;//to calculate the total file number
			 // System.out.println(tlength);
		  } 
	}
	
	public static class testReduce extends Reducer< Text,Text, Text, Text> { 
		private Text result = new Text();
		private Text text1=new Text();
		private NullWritable ob;
		private double[] times=new double[uwords.length];
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			int sum = 0,i; 
			String temp,tkey;
			String[] temps;
			String sign1="",outr="";
			tkey=key.toString();
			if(tkey.contains("!")) {//it's the key-value pair to record file numbers
				for(Text val: values)
					for(i=0;i<uwords.length;i++)
						if(tkey.split("!")[1].equals(uwords[i])) idf_test[i]+=Double.parseDouble(val.toString());
			}
			else {// it's the key-value pair to cal tf
				for(i=0;i<uwords.length;i++) times[i]=0;
				for (Text val : values) { 
					temp=val.toString();
					if(temp.contains("@")) {//the freq of a word
						temps=temp.split("@");
						for(i=0;i<uwords.length;i++) if(temps[1].equals(uwords[i])) times[i]+=Double.parseDouble(temps[0]);
					}else {
						sum+=Double.parseDouble(val.toString());
					}
				}
				for(i=0;i<uwords.length;i++) times[i]=times[i]/sum;
				for(i=0;i<uwords.length;i++) outr=outr+times[i]+" ";
				//System.out.println(outr);
				result.set(outr);
				context.write(key,result); //output<tf,code>
			}
			
		} 
	}
	
	public static class testMap2 extends Mapper<Object, Text, Text,Text> {  
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  String line = value.toString();
			  //System.out.println(line);
			  String[] lines = line.split("\t");
			  context.write(new Text(lines[0]), new Text(lines[1]));//read the tf values
		  } 
	}
	
	public static class testReduce2 extends Reducer< Text,Text, NullWritable, Text> { 
		private Text result = new Text();
		private Text text1=new Text();
		private NullWritable ob;
		private double[] times=new double[uwords.length];
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String tkey=key.toString();
			String[] ss;
			for(Text val:values) {
				ss=val.toString().split(" ");
				for(int i=0;i<ss.length;i++) times[i]=Double.parseDouble(ss[i])*idf_test[i];//the final result
				String temp="";
				for(int i=0;i<ss.length;i++)  temp=temp+times[i]+" ";
				temp+=tkey;
				result.set(temp);
				//System.out.println(temp);
				context.write(ob,result); 
			}
		}
	} 
	
	public static void main(String[] args) throws Exception { 
		Configuration conf1 = new Configuration();
		Path mypath = new Path(args[1]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
            System.out.println("previous output path of tf(test) removed!");
        } 
        for(int i=0;i<uwords.length;i++) mapf.put(uwords[i], null);
		Job job1 = new Job(conf1,"tf_idf_test");
		job1.setJarByClass(Tf_idf_test.class); 
		job1.addCacheFile(new Path(args[2]).toUri());
		job1.setMapperClass(testMap.class); 
		job1.setMapOutputKeyClass(Text.class); 
		job1.setMapOutputValueClass(Text.class); 
		job1.setReducerClass(testReduce.class); 
		job1.setOutputKeyClass(Text.class); 
		job1.setOutputValueClass(Text.class); 
		job1.setInputFormatClass(TextInputFormat.class); 
		job1.setOutputFormatClass(TextOutputFormat.class); 
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		if((job1.waitForCompletion(true)?true:false)){
  			System.out.println("get the primal vector for test data!");
  		}
		for(int i=0;i<idf_test.length;i++) idf_test[i]=(double)Math.log(tlength/(idf_test[i]+1.0))/Math.log(10);
		//System.out.println(Arrays.toString(idf_test));//here is the idf value of these words
		
		Configuration conf2 = new Configuration();
		Path mypath2 = new Path(args[3]); 
  		FileSystem hdfs2 = mypath2.getFileSystem(conf2);  
        if (hdfs2.isDirectory(mypath2)) {  
            hdfs2.delete(mypath2, true);  
            System.out.println("previous output path of tf-idf(test) removed!");
        } 
        for(int i=0;i<uwords.length;i++) mapf.put(uwords[i], null);
		Job job2 = new Job(conf1,"tf_idf_test");
		job2.setJarByClass(Tf_idf_test.class); 
		job2.setMapperClass(testMap2.class); 
		job2.setMapOutputKeyClass(Text.class); 
		job2.setMapOutputValueClass(Text.class); 
		job2.setReducerClass(testReduce2.class); 
		job2.setOutputKeyClass(NullWritable.class); 
		job2.setOutputValueClass(Text.class); 
		job2.setInputFormatClass(TextInputFormat.class); 
		job2.setOutputFormatClass(TextOutputFormat.class); 
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		if((job2.waitForCompletion(true)?true:false)){
  			System.out.println("get the final vector for test data!");
  		}
	}
}

