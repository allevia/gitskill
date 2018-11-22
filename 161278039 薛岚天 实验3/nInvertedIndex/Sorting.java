package nInvertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;


public class Sorting {
	public static class Myk implements WritableComparable<Myk>{
		private String key1;
		private String key2;
		
		public void setkey1(String strName) {
	        this.key1 = strName;
	    }
	    public void setkey2(String strValue) {
	        this.key2= strValue;
	    }
	    
	    public  String getkey1() {
	        return this.key1;
	    }
	    public  String getkey2() {
	        return this.key2;
	    }
	
		@Override
	    public void write(DataOutput out) throws IOException {
	        out.writeUTF(key1);
	        out.writeUTF(key2);
		}
		 @Override
		 public void readFields(DataInput in) throws IOException {
		    key1 =in.readUTF();
		    key2 =in.readUTF();
		 }
		 @Override
		 public int compareTo(Myk o) {
			if(!key1.equals(o.key1)) {
				return key1.compareTo(o.key1);
			}else {
				int a1=Integer.parseInt(key2);
				int a2=Integer.parseInt(o.key2);
				if(a1!=a2)
					return a1<a2?1:-1;
				else return 0;
			}
		}
		 @Override
		 public int hashCode() {
			 return key1.hashCode();
		 }
	}
	//map
	public static class SortMap extends Mapper<Object, Text, Myk, Text> { 
		private Text result = new Text();
		private Myk word= new Myk();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			word.setkey1(line.split("@")[0]);
			word.setkey2(line.split("#")[1]);
			result.set(line.split("@")[1]);
			context.write(word, result);
		} 
	}
	
	
	public static class NewPartitioner extends Partitioner<Myk, Text> {
  		@Override
  		public int getPartition(Myk key, Text value, int numReduceTasks) {
  			return Math.abs(key.getkey1().hashCode() * 127)% 31;
  		}
  	}
	
	//reduce
	public static class SortReduce extends Reducer<Myk, Text, Text, Text> {
		private Text result = new Text();
		private Text separator=new Text("----------");
		public void reduce(Myk key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			result.set(key.getkey1()+" "+key.getkey2());
			for (Text val : values) {
				context.write(result, val);
				context.write(separator,null);
			}
		} 
	}
}

