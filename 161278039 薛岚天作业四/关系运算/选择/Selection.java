package relation;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//choose the required value among all the tuples 
public class Selection {
	public static class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable>{
		private int id;
		private String value;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			id = context.getConfiguration().getInt("col", 0);
			value = context.getConfiguration().get("value");
		}
		
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws 
		IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			Boolean cc=Boolean.getBoolean("greater");
			if(cc) {
				if(record.isgreater(id, value)) 
					context.write(record, NullWritable.get());
			}else if(record.isCondition(id, value))
					context.write(record, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job selectionJob = new Job();
		selectionJob.setJobName("selectionJob");
		selectionJob.setJarByClass(Selection.class);
		selectionJob.getConfiguration().setInt("col", Integer.parseInt(args[2]));
		selectionJob.getConfiguration().set("value", args[3]);
		
		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(RelationA.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);

		selectionJob.setNumReduceTasks(0);

		selectionJob.setInputFormatClass(TextInputFormat.class);
		selectionJob.setOutputFormatClass(TextOutputFormat.class);
		//the "greater" variable used to determine choose equal or great
		System.getProperty("greater");
		FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
		
		selectionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}

