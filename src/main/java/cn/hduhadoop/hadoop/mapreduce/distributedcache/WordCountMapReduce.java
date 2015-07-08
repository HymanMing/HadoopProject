package cn.hduhadoop.hadoop.mapreduce.distributedcache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMapReduce extends Configured implements Tool {

	// Mapper class
	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private Text mapOutPutKey = new Text();
		private final static LongWritable mapOutPutValue = new LongWritable(1L);
		
		List<String> list = new ArrayList<String>();
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			
			// get cache file uri
			@SuppressWarnings("deprecation")
			URI[] uris = DistributedCache.getCacheFiles(conf);
			
			Path path = new Path(uris[0]);
			
			// get FileSystem
			FileSystem fs = path.getFileSystem(conf);
			
			// get inputstream
			FSDataInputStream inStream = fs.open(path);
			
			InputStreamReader isr = new InputStreamReader(inStream);
			
			BufferedReader bf = new BufferedReader(isr);
			
			String line;
			// get cache file and then save in list
			while ((line = bf.readLine()) != null) {
				if (line.trim().length() > 0) {
					list.add(line);
				}
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(lineValue);
			while (tokenizer.hasMoreTokens()) {
				String wordValue = tokenizer.nextToken();
				
				// compare whether wordvalue in cache file
				if (list.contains(wordValue)) {
					continue;
				}
				
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
	public static class WordCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable outPutValue = new LongWritable();
		
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

			outPutValue.set(sum);
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
		
		URI uri = new URI("/user/hyman/mr/cache/word.data");
		
		// add calche file
		DistributedCache.addCacheFile(uri, conf);
		
		// create job 
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		
		// set job
		
		// 	1) set class jar
		job.setJarByClass(this.getClass());
		
		//	2) set input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//	3) set mapper class
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//	4) set shuffle
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setSortComparatorClass(LongWritable.Comparator.class);
		//job.setCombinerClass(ModuleReducer.class);
		//job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//	5) set reducer
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//	6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/cache/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/cache/output"
		};
		
		// run job
		int status = ToolRunner.run(new WordCountMapReduce(), args);
		
		// exit program
		System.exit(status);
	}
}
