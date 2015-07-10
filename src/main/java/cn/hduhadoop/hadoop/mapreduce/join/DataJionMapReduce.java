package cn.hduhadoop.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class DataJionMapReduce extends Configured implements Tool {

	// Mapper class
	public static class DataJionMapper extends
			Mapper<LongWritable, Text, LongWritable, DataWritable> {
		private LongWritable mapOutputKey = new LongWritable();
		private DataWritable mapOutputValue = new DataWritable();

		private String name = null;
		private Long id = 0L;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// get line fields
			String lineValue = value.toString();
			String[] fields = lineValue.split(",");

			// set map output key
			id = Long.valueOf(fields[0]);
			mapOutputKey.set(id);

			name = fields[1];

			if (3 == fields.length) {// customer
				String telePhone = fields[2];
				// set map output value
				mapOutputValue.set("customer", name + "," + telePhone);
			} else if (4 == fields.length) { // order
				Float price = Float.valueOf(fields[2]);
				String date = fields[3];
				// set map output value
				mapOutputValue.set("order", name + "," + price + "," + date);
			}

			context.write(mapOutputKey, mapOutputValue);
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

	}

	// Reducer class
	public static class DataJionReducer extends
			Reducer<LongWritable, DataWritable, NullWritable, Text> {
		private NullWritable outPutKey = NullWritable.get();
		private Text outPutValue = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(LongWritable key, Iterable<DataWritable> values,
				Context context) throws IOException, InterruptedException {
			String customerInfo = null;
			List<String> orderList = new ArrayList<String>();

			for (DataWritable value : values) {
				if ("customer".equals(value.getTag())) {
					customerInfo = value.getData();
				} else if ("order".equals(value.getTag())) {
					orderList.add(value.getData());
				}
			}

			if (null == customerInfo || 0 == orderList.size()) {
				return;
			}

			for (String order : orderList) {
				// ser output value
				outPutValue.set(key.toString() + "," + customerInfo + ","
						+ order);
				
				// output
				context.write(outPutKey, outPutValue);
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

		// 1) set class jar
		job.setJarByClass(this.getClass());

		// 2) set input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 3) set mapper class
		job.setMapperClass(DataJionMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DataWritable.class);

		// 4) set shuffle
		// job.setPartitionerClass(HashPartitioner.class);
		// job.setSortComparatorClass(LongWritable.Comparator.class);
		// job.setCombinerClass(ModuleReducer.class);
		// job.setGroupingComparatorClass(LongWritable.Comparator.class);

		// 5) set reducer
		job.setReducerClass(DataJionReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://10.1.16.251:8020/user/hyman/mr/join/input",
				"hdfs://10.1.16.251:8020/user/hyman/mr/join/output" };

		// run job
		int status = ToolRunner.run(new DataJionMapReduce(), args);

		// exit program
		System.exit(status);
	}

}
