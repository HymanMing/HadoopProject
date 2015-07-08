package cn.hduhadoop.hadoop.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortMapReduce extends Configured implements Tool {

	// Mapper class
	public static class SecondarySortMapper extends
			Mapper<LongWritable, Text, IntPairWritable, IntWritable> {
		
		private IntPairWritable mapOutputKey = new IntPairWritable();
		private IntWritable mapOutputValue = new IntWritable();
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			String[] splited = lineValue.split("\t");
			int first = Integer.valueOf(splited[0]);
			int second = Integer.valueOf(splited[1]);
			
			mapOutputKey.set(first, second);
			mapOutputValue.set(second);
			
			context.write(mapOutputKey, mapOutputValue);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

	}

	// Reducer class
	public static class SecondarySortReducer extends
			Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable outputKey = new IntWritable();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(IntPairWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			outputKey.set(key.getFirst());
			for (IntWritable value : values) {
				context.write(outputKey, value);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
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
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//	4) set shuffle
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(IntPairWritable.Comparator.class);
		//job.setCombinerClass(ModuleReducer.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		//	5) set reducer
		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//	6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/secondarysort/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/secondarysort/output"
		};
		
		// run job
		int status = ToolRunner.run(new SecondarySortMapReduce(), args);
		
		// exit program
		System.exit(status);
	}
	

}
