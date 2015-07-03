package cn.hduhadoop.hadoop.mapreduce.app;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class AverageValueMapReduce extends Configured implements Tool {

	// Mapper class
	public static class AverageValueMapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text mapOutPutKey = new Text();
		private FloatWritable mapOutPutValue = new FloatWritable();
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String stuName = null;
			String stuScore = null;
			
			String lineValue = value.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(lineValue);
			
			while (tokenizer.hasMoreTokens()) {
				stuName = tokenizer.nextToken();
				stuScore = tokenizer.nextToken();
				
				mapOutPutKey.set(stuName);
				mapOutPutValue.set(Float.valueOf(stuScore));
				System.out.println(stuName + "-----" + stuScore);
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
	public static class AverageValueReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable outPutValue = new FloatWritable(); 
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			
			//int average = 0;
			for (FloatWritable value : values) {
				sum += value.get();
				count++;
			}		
			outPutValue.set(sum / count);
			
			context.write(key, outPutValue);
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
		job.setMapperClass(AverageValueMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		//	4) set shuffle
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setSortComparatorClass(LongWritable.Comparator.class);
		//job.setCombinerClass(AverageValueReducer.class);
		//job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//	5) set reducer
		job.setReducerClass(AverageValueReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		//	6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/average/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/average/output3"
		};
		
		// run job
		int status = ToolRunner.run(new AverageValueMapReduce(), args);
		
		// exit program
		System.exit(status);
	}

}
