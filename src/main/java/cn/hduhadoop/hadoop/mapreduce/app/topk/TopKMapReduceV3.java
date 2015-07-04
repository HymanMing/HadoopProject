package cn.hduhadoop.hadoop.mapreduce.app.topk;

import java.io.IOException;
import java.util.TreeSet;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKMapReduceV3 extends Configured implements Tool {
	private static final int KEY = 2;

	// Mapper class
	public static class TopKMapperV3 extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {

		private TreeSet<Long> topK = new TreeSet<Long>();

		private LongWritable mapOutPutKey = new LongWritable();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			if (null == lineValue) {
				return;
			}
			long tempValue = Long.valueOf(lineValue);

			topK.add(tempValue);
			if (topK.size() > KEY) {
				topK.remove(topK.first());
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Long top : topK) {
				mapOutPutKey.set(top);
				context.write(mapOutPutKey, NullWritable.get());
			}
		}

	}

	// Reducer class
	public static class TopKReducerV3 extends
			Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		private TreeSet<Long> topK = new TreeSet<Long>();
		private LongWritable outPutKey = new LongWritable();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			topK.add(key.get());
			if (topK.size() > KEY) {
				topK.remove(topK.first());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Long top : topK) {
				outPutKey.set(top);
				context.write(outPutKey, NullWritable.get());
			}
		}

	}

	// Driver
	public int run(String[] args) throws Exception {

		// set conf
		Configuration conf = super.getConf();

		// create job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());

		// set job

		// 1) set class jar
		job.setJarByClass(this.getClass());

		// 2) set input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 3) set mapper class
		job.setMapperClass(TopKMapperV3.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 4) set shuffle
		// job.setPartitionerClass(HashPartitioner.class);
		// job.setSortComparatorClass(LongWritable.Comparator.class);
		// job.setCombinerClass(ModuleReducer.class);
		// job.setGroupingComparatorClass(LongWritable.Comparator.class);

		// 5) set reducer
		job.setReducerClass(TopKReducerV3.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		// job.setNumReduceTasks(0);

		// 6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://10.1.16.251:8020/user/hyman/mr/topk3/input",
				"hdfs://10.1.16.251:8020/user/hyman/mr/topk3/output" };

		// run job
		int status = ToolRunner.run(new TopKMapReduceV3(), args);

		// exit program
		System.exit(status);
	}

}
