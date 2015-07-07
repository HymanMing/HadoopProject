package cn.hduhadoop.hadoop.mapreduce.output;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.swing.plaf.multi.MultiOptionPaneUI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleOutPutMapReduce extends Configured implements Tool {

	// Mapper class
	public static class MultipleOutPutMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		private Text mapOutputKey = new Text();
		private NullWritable nullWritable = NullWritable.get();
		private MultipleOutputs<Text, NullWritable> mop;
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			mop = new MultipleOutputs<Text, NullWritable>(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(lineValue);
			while (tokenizer.hasMoreTokens()) {
				String wordValue = tokenizer.nextToken();
				mapOutputKey.set(wordValue);
				
				String firstChar = wordValue.substring(0, 1);
				if (firstChar.matches("[a-z]")) {
					mop.write("az", mapOutputKey, nullWritable);
				} else if (firstChar.matches("[A-Z]")) {
					mop.write("AZ", mapOutputKey, nullWritable);
				} else if (firstChar.matches("[0-9]")) {
					mop.write("09", mapOutputKey, nullWritable);
				} else {
					mop.write("other", mapOutputKey, nullWritable);
				}
				
			}
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
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//	3) set mapper class
		job.setMapperClass(MultipleOutPutMapper.class);
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
		
		MultipleOutputs.addNamedOutput(job, "az", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "AZ", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "09", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "other", TextOutputFormat.class, Text.class, NullWritable.class);
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/output/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/output/output"
		};
		
		// run job
		int status = ToolRunner.run(new MultipleOutPutMapReduce(), args);
		
		// exit program
		System.exit(status);
	}

}
