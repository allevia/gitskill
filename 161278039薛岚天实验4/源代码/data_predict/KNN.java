package data_predict;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KNN {
	public static class KNNMap extends Mapper<Object,Text,Text,Text>{
		private int k;
		private URI[] localFiles;
		private ArrayList<Instance> trainSet;
		
		@Override
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			k = conf.getInt("k", 1);
			//System.out.println(k);
			trainSet = new ArrayList<Instance>();
			
			localFiles = Job.getInstance(conf).getCacheFiles();
			for (URI localFile : localFiles) {
				Path patternsPath = new Path(localFile.getPath());
				String line=null;
				BufferedReader br =new BufferedReader(new FileReader(patternsPath.getName().toString()));
				while ((line = br.readLine()) != null) {
					//System.out.println(line);
					Instance trainInstance = new Instance(line);
					trainSet.add(trainInstance);
					//System.out.print(trainInstance.getAttributeValue().length);
					//System.out.println(trainInstance.getLable());
				  }
			  }
		} 
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//distance stores all the current nearest distance value
			// trainLable store the corresponding labels
			String line=value.toString();
			String[]lines=line.split(" ");
			ArrayList<Double> distance = new ArrayList<Double>(k);
			ArrayList<String> trainLable = new ArrayList<String>(k);
			for(int i = 0;i < k;i++){
				distance.add(Double.MAX_VALUE);
				trainLable.add("null");
			}	
			Instance testInstance = new Instance(line.toString());
			//System.out.println(line);
			for(int i = 0;i < trainSet.size();i++){
				try {
					double dis = EuclideanDistance(trainSet.get(i).getAttributeValue(), testInstance.getAttributeValue());
					int index = indexOfMax(distance);
					//System.out.println(index);
					if(dis < distance.get(index)){
						distance.remove(index);
					    trainLable.remove(index);
					    distance.add(dis);
					    trainLable.add(trainSet.get(i).getLable());
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}
			String temp="";
			for(int i=0;i<trainLable.size();i++) temp=temp+trainLable.get(i).toString()+",";
			Text tt=new Text(temp);
			Text sym=new Text(lines[lines.length-1]);
			//System.out.print(tt.toString()+" "+sym.toString()+"\n");
		    context.write(sym, tt);
		}
		
		public double EuclideanDistance(double[] a,double[] b) throws Exception{
			if(a.length != b.length)
				throw new Exception("size not compatible!");
			double sum = 0.0;
	        for(int i = 0;i < a.length;i++){
				sum += Math.pow(a[i] - b[i], 2);
	        }
			return Math.sqrt(sum);
		}
		
		public int indexOfMax(ArrayList<Double> array){
			int index = 0;
			Double max =array.get(0); 
			for (int i = 0;i < array.size();i++){
				if(array.get(i) > max){
					max = array.get(i);
					index = i;
				}
			}
			return index;
		}
	}
	
	public static class KNNReduce extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key, Iterable<Text> kLables, Context context) throws IOException, InterruptedException{
			int ip=0,in=0,ineu=0;
			Text predict = new Text();
			for(Text val: kLables){
				String[] labels=val.toString().split(",");
				for(int i=0;i<labels.length;i++) {
					if(labels[i].equals("pos")) ip+=1;
					else if(labels[i].equals("neu")) ineu+=1;
					else in+=1;
				}
			}
			if(ip>in) {
				if(ip>ineu) predict.set("pos");
				else predict.set("neu");
			}else {
				if(in>ineu) predict.set("neg");
				else predict.set("neu");
			}
			context.write(key, predict);
		}
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf1 = new Configuration();
		Path mypath = new Path(args[1]); 
  		FileSystem hdfs = mypath.getFileSystem(conf1);  
        if (hdfs.isDirectory(mypath)) {  
            hdfs.delete(mypath, true);  
            System.out.println("previous output path of tf-idf prediction removed!");
        } 
        
		Job kNNJob = new Job();
		kNNJob.setJobName("kNNJob");
		
		kNNJob.setJarByClass(KNN.class);
		kNNJob.addCacheFile(new Path(args[2]).toUri());
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));
		
		kNNJob.setMapperClass(KNNMap.class);
		kNNJob.setMapOutputKeyClass(Text.class);
		kNNJob.setMapOutputValueClass(Text.class);

		kNNJob.setReducerClass(KNNReduce.class);
		kNNJob.setOutputKeyClass(Text.class);
		kNNJob.setOutputValueClass(Text.class);

		kNNJob.setInputFormatClass(TextInputFormat.class);
		kNNJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
		
		if((kNNJob.waitForCompletion(true)?true:false)){
  			System.out.println("predict using KNN finished!");
  		}
	}
}
