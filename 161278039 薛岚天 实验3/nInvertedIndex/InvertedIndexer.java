package nInvertedIndex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;

import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * the commented codes are that used to run on the download_data, which
 * has been tested in small data set, however run the whole program based on 
 * download_data requires too much time, so the version now can be ran on
 * the fulldata.txt
 */
public class InvertedIndexer {
  public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
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
		  //FileSplit fileSplit = (FileSplit)context.getInputSplit();
		  //String fileName = fileSplit.getPath().getName(); //get filename of the split
		  String line = value.toString();
		  //String[] lines = line.split("  ");
		  String[] lines = line.split("\t");
		  String title="";
		  String theline="";
		  String theurl="";
		  
		  /*if(lines.length==5) {
			  theline = lines[3];
			  theurl = lines[4];
		  }else if(lines.length==6) {
			  theline=lines[3]+","+lines[4];
			  theurl=lines[5];
		  }else return;*/
		  if(lines.length==6) {
			  title=lines[0]+lines[1];
			  theline = lines[4];
			  theurl=lines[5];
		  }	
		  else if(lines.length==7) {
			  title=lines[0]+lines[1];
			  theline=lines[4]+","+lines[5];
			  theurl=lines[6];
			}	
		  else return;
		  
		  for(String pattern : stopwords){
			  theline = theline.replace(pattern,""); //skip the stop-word
		  }
		  Result words = ToAnalysis.parse(theline);
		  
		  for(int i=0;i<words.size();i++) {
			  String wor=words.get(i).toString();
			  if(!wor.contains("/")) continue; // skip the signs
			  if (wor.endsWith("en") || wor.endsWith("m"))continue; //skip English and number
			  //word.set(wor+"@"+fileName+"#"+theurl); 
			  word.set(wor+"@"+title+"#"+theurl);
			  context.write(word, one); 
		  }
	  } 
  }


  /** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer **/
  	public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
  		@Override
  		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
  			String term = new String();
  			term = key.toString().split("@")[0]; // <term#docid>=>term
  			return super.getPartition(new Text(term), value, numReduceTasks);
  		}
  	}

  	public static class InvertedIndexReducer extends Reducer<Text,IntWritable, Text, Text> {
  		private Text word1 = new Text();
  		private Text word2 = new Text();
  		String temp = new String();
  		static Text CurrentItem = new Text(" ");
  		static List<String> postingList = new ArrayList<String>();
  		@Override
  		public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
  			int sum = 0;
  			word1.set(key.toString().split("#")[0]);
  			temp = key.toString().split("#")[1];
  			for (IntWritable val : values) {
  				sum += val.get();
  			}
  			String a=sum+"";
  			word2.set("<" + temp + "," + a + ">");
  			if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
  				StringBuilder out = new StringBuilder();
  				long count = 0;
  				for (String p : postingList) {
  					out.append(p);
  					String ss=p.split(",")[1].split(">")[0];
  					int ii=Integer.parseInt(ss);
  					out.append(";");
  					count +=ii;
  				}
  				out.append("total#" + count);
  				if (count > 0) {
  					context.write(CurrentItem, new Text(out.toString()));
  				}
  				postingList = new ArrayList<String>();
  			}
  			CurrentItem = new Text(word1);
  			postingList.add(word2.toString()); // 不断向postingList也就是文档名称中添加词表
  		}

  		// cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况
  		public void cleanup(Context context) throws IOException,InterruptedException {
  			StringBuilder out = new StringBuilder();
  			long count = 0;
  			for (String p : postingList) {
  				out.append(p);
  				String ss=p.split(",")[1].split(">")[0];
				int ii=Integer.parseInt(ss);
				out.append(";");
				count +=ii;
  			}
  			out.append("total#" + count);
  			if (count > 0)
  				context.write(CurrentItem, new Text(out.toString()));
  		}
  	}

  	public static void main(String[] args) throws Exception {
  		Configuration conf1 = new Configuration();
  		Path mypath = new Path(args[1]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
        } 
        Path mypath1 = new Path(args[3]); 
  		FileSystem hdfs1 = mypath1.getFileSystem(conf1);  
        if (hdfs1.isDirectory(mypath1)) {  
            hdfs1.delete(mypath1, true);  
        }
     
  		Job job1 = new Job(conf1, "inverted index");
  		job1.addCacheFile(new Path(args[2]).toUri());
  		job1.setJarByClass(InvertedIndexer.class);
  		
  		job1.setMapperClass(InvertedIndexMapper.class);
  		job1.setMapOutputKeyClass(Text.class);
  		job1.setMapOutputValueClass(IntWritable.class);
  		job1.setReducerClass(InvertedIndexReducer.class);
  		job1.setPartitionerClass(NewPartitioner.class);
  		job1.setInputFormatClass(TextInputFormat.class);
  		job1.setOutputFormatClass(TextOutputFormat.class); 
  		
  		job1.setOutputKeyClass(Text.class);
  		job1.setOutputValueClass(Text.class);
  		FileInputFormat.addInputPath(job1, new Path(args[0]));
  		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
  		if((job1.waitForCompletion(true)?true:false)){
  			System.out.println("job1 finished!");
  		}
  		
  		Configuration conf2 = new Configuration(); //sort
		Job job2 = new Job(conf2,"sorting");
		job2.setJarByClass(Sorting.class); 
		
		job2.setMapOutputKeyClass(Sorting.Myk.class); 
		job2.setMapOutputValueClass(Text.class); 
		
		job2.setOutputKeyClass(Text.class); 
		job2.setOutputValueClass(Text.class); 
		job2.setMapperClass(Sorting.SortMap.class); 
		job2.setPartitionerClass(Sorting.NewPartitioner.class);
		job2.setReducerClass(Sorting.SortReduce.class); 
		job2.setInputFormatClass(TextInputFormat.class); 
		job2.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		if(job2.waitForCompletion(true)?true:false)
			System.out.println("sorting finished");
  	}
}