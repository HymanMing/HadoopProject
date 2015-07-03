package cn.hduhadoop.hadoop.mapreduce.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class DataTotalMapReduce extends Configured implements Tool {

	// Mapper class
	public static class DataTotalMapper extends
			Mapper<LongWritable, Text, Text, DataWritable> {
		private Text mapOutPutKey = new Text();
		private DataWritable mapOutPutValue = new DataWritable();
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			String[] data = lineValue.split("\t");
			
			String phoneNum = data[1];
			long upPackNum = Long.valueOf(data[6]);
			long downPackNum = Long.valueOf(data[7]);
			long upPayLoad = Long.valueOf(data[8]);
			long downPayLoad = Long.valueOf(data[9]);
			
			// set
			mapOutPutKey.set(phoneNum);
			mapOutPutValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			
			//context write
			context.write(mapOutPutKey, mapOutPutValue);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

	}

	// Reducer class
	public static class DataTotalReducer extends
			Reducer<Text, DataWritable, Text, DataWritable> {
		
		private DataWritable outPutValue = new DataWritable();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<DataWritable> values,
				Context context) throws IOException, InterruptedException {
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			
			for (DataWritable value : values) {
				upPackNum += value.getUpPackNum();
				downPackNum += value.getDownPackNum();
				upPayLoad += value.getUpPayLoad();
				downPayLoad += value.getDownPayLoad();
			}
			
			// set 
			outPutValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			
			// context write
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
		job.setMapperClass(DataTotalMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataWritable.class);
		
		//	4) set shuffle
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setSortComparatorClass(LongWritable.Comparator.class);
		//job.setCombinerClass(DataTotalReducer.class);
		//job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//	5) set reducer
		job.setReducerClass(DataTotalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataWritable.class);
		
		//	6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
			"hdfs://10.1.16.251:8020/user/hyman/mr/datatoal/input",
			"hdfs://10.1.16.251:8020/user/hyman/mr/datatoal/output2"
		};
		
		// run job
		int status = ToolRunner.run(new DataTotalMapReduce(), args);
		
		// exit program
		System.exit(status);
	}

}
