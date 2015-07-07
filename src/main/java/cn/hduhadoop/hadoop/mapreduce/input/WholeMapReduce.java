package cn.hduhadoop.hadoop.mapreduce.input;

import java.awt.image.MultiPixelPackedSampleModel;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WholeMapReduce extends Configured implements Tool {

	// Mapper class
	public static class WholeMapper extends
			Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text fileNameKey;
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			fileNameKey = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
		}

		@Override
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(fileNameKey, value);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

	}

	// Driver
	public int run(String[] args) throws Exception {
		
		// set conf
		Configuration conf = super.getConf();
		
		// create job 
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		
		// set job
		
		// 	1) set class jar
		job.setJarByClass(this.getClass());
		
		//	2) set input
		job.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//	3) set mapper class
		job.setMapperClass(WholeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//	4) set shuffle
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setSortComparatorClass(LongWritable.Comparator.class);
		//job.setCombinerClass(ModuleReducer.class);
		//job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//	5) set reducer
		//job.setReducerClass(ModuleReducer.class);
		//job.setOutputKeyClass(LongWritable.class);
		//job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		//	6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/inputformat/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/inputformat/output"
		};
		
		// run job
		int status = ToolRunner.run(new WholeMapReduce(), args);
		
		// exit program
		System.exit(status);
		
	}

}
