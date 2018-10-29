package relation;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Difference {
	public static class DifferenceMap extends Mapper<Object, Text, RelationA, Text>{
		@Override
		protected void map(Object key,Text value, Context context)throws 
		IOException, InterruptedException{
			FileSplit split=(FileSplit)context.getInputSplit();
			String filename = split.getPath().getName();
			RelationA record = new RelationA(value.toString());
			context.write(record, new Text(filename));
		}
	}
	
	public static class DifferenceReduce extends Reducer<RelationA, Text, RelationA, NullWritable>{
		String setR;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			setR = context.getConfiguration().get("setR");
		}
		@Override
		public void reduce(RelationA key, Iterable<Text> value, Context context) throws 
		IOException,InterruptedException{
			for(Text val : value){
				if(!val.toString().equals(setR))//if the key not in setR, do not record
					return ;
			}
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job differenceJob = new Job();
		differenceJob.setJobName("differenceJob");
		differenceJob.setJarByClass(Difference.class);
		differenceJob.getConfiguration().set("setR", args[2]);
		
		differenceJob.setMapperClass(DifferenceMap.class);
		differenceJob.setMapOutputKeyClass(RelationA.class);
		differenceJob.setMapOutputValueClass(Text.class);

		differenceJob.setReducerClass(DifferenceReduce.class);
		differenceJob.setOutputKeyClass(RelationA.class);
		differenceJob.setOutputValueClass(NullWritable.class);

		differenceJob.setInputFormatClass(TextInputFormat.class);
		differenceJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(differenceJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(differenceJob, new Path(args[1]));
		
		differenceJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
