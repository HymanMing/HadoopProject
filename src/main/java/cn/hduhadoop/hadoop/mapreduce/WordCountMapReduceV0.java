package cn.hduhadoop.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapReduceV0 {

	public static enum WORDCOUNT_COUNTERS{
		MAP_INPUT_RECORD_COUNTER
	}
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private Text mapOutPutKey = new Text();
		private LongWritable mapOutPutValue = new LongWritable(1L);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter(WORDCOUNT_COUNTERS.MAP_INPUT_RECORD_COUNTER);
			counter.increment(1L);
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
				"hdfs://10.1.16.251:8020/user/hyman/mr/wc/input",
				"hdfs://10.1.16.251:8020/user/hyman/mr/wc/mapoutput"
		};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf,
				WordCountMapReduceV0.class.getSimpleName());

		job.setJarByClass(WordCountMapReduceV0.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//job.setNumReduceTasks(0);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean isSuccess = job.waitForCompletion(true);

		System.exit(isSuccess ? 0 : 1);

	}
}
