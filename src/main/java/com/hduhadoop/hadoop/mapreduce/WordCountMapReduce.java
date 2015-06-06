package com.hduhadoop.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapReduce {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private Text mapOutPutKey = new Text();
		private LongWritable mapOutPutValue = new LongWritable(1L);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(lineValue);
			while (tokenizer.hasMoreTokens()) {
				String wordValue = tokenizer.nextToken();
				mapOutPutKey.set(wordValue);
				context.write(mapOutPutKey, mapOutPutValue);
			}
		}

	}

	public static class MyReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable outPutValue = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}

			outPutValue.set(sum);
			context.write(key, outPutValue);
		}

	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://10.1.16.251:8020/user/hyman/mr/input",
				"hdfs://10.1.16.251:8020/user/hyman/mr/output1"
		};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf,
				WordCountMapReduce.class.getSimpleName());

		job.setJarByClass(WordCountMapReduce.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean isSuccess = job.waitForCompletion(true);

		System.exit(isSuccess ? 0 : 1);

	}
}
