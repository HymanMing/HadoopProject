package cn.hduhadoop.hadoop.mapreduce.app.topk;

import java.io.IOException;

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

public class TopKMapReduce extends Configured implements Tool {

	// Mapper class
	public static class TopKMapper extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {
		private long MaxValue = Long.MIN_VALUE;
		private long MinValue = Long.MAX_VALUE;
		
		private LongWritable outPutKey = new LongWritable();
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
				return ;
			}
			long tempValue = Long.valueOf(lineValue);
			if (MaxValue < tempValue) {
				MaxValue = tempValue;
			}
			if (MinValue > tempValue) {
				MinValue = tempValue;
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			outPutKey.set(MaxValue);		
			context.write(outPutKey, NullWritable.get());
			outPutKey.set(MinValue);		
			context.write(outPutKey, NullWritable.get());
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
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
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
			"hdfs://10.1.16.251:8020/user/hyman/mr/topk1/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/topk1/output3"
		};
		
		// run job
		int status = ToolRunner.run(new TopKMapReduce(), args);
		
		// exit program
		System.exit(status);
	}

}
