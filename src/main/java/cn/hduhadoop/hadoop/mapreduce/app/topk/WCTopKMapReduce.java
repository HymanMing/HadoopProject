package cn.hduhadoop.hadoop.mapreduce.app.topk;

import java.io.IOException;
import java.util.StringTokenizer;
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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WCTopKMapReduce extends Configured implements Tool {

	// Mapper class
	public static class WCTopKMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private Text mapOutPutKey = new Text();
		private LongWritable mapOutPutValue = new LongWritable(1L);
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(lineValue);
			while (tokenizer.hasMoreTokens()) {
				String wordValue = tokenizer.nextToken();
				mapOutPutKey.set(wordValue);
				context.write(mapOutPutKey, mapOutPutValue);
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

	}

	// Reducer class
	public static class WCTopKReducer extends
			Reducer<Text, LongWritable, WCTopKWritable, NullWritable> {
		private static final int KEY = 2;
		private TreeSet<WCTopKWritable> topK = new TreeSet<WCTopKWritable>();
		//private WCTopKWritable wcTopKWritable = new WCTopKWritable();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			
			WCTopKWritable wcTopKWritable = new WCTopKWritable(key.toString(), sum);
			
			topK.add(wcTopKWritable);
			
			if (topK.size() > KEY) {
				topK.remove(topK.first());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (WCTopKWritable top : topK) {
				context.write(top, NullWritable.get());
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
		
		// 	1) set class jar
		job.setJarByClass(this.getClass());
		
		//	2) set input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//	3) set mapper class
		job.setMapperClass(WCTopKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//	4) set shuffle
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setSortComparatorClass(LongWritable.Comparator.class);
		//job.setCombinerClass(ModuleReducer.class);
		//job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//	5) set reducer
		job.setReducerClass(WCTopKReducer.class);
		job.setOutputKeyClass(WCTopKWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//	6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/topkey/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/topkey/output"
		};
		
		// run job
		int status = ToolRunner.run(new WCTopKMapReduce(), args);
		
		// exit program
		System.exit(status);
	}

}
