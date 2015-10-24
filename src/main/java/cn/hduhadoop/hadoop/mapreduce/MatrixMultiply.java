package cn.hduhadoop.hadoop.mapreduce;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 实现矩阵相乘，MatrixMultiply实现了Tool接口，调用对应的run方法运行job
 * 
 * @author GUI
 * 
 */
public class MatrixMultiply extends Configured implements Tool {
	/** mapper和reducer需要的三个必要变量，由conf.get()方法得到 **/
	public static int rowM = 0;
	public static int columnM = 0;
	public static int columnN = 0;

	// Mapper class
	public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {

		private Text map_key = new Text();
		private Text map_value = new Text();

		/**
		 * 执行map()函数前先由conf.get()得到main函数中提供的必要变量， 这也是MapReduce中共享变量的一种方式
		 */
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			columnN = Integer.parseInt(conf.get("columnN"));
			rowM = Integer.parseInt(conf.get("rowM"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			/** 得到输入文件名，从而区分输入矩阵M和N **/
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();

			if (fileName.contains("M")) {
				String[] tuple = value.toString().split(",");
				int i = Integer.parseInt(tuple[0]);
				//String[] tuples = tuple[1].split("\t");
				int j = Integer.parseInt(tuple[1]);
				int Mij = Integer.parseInt(tuple[2]);

				for (int k = 1; k < columnN + 1; k++) {
					map_key.set(i + "," + k);
					map_value.set("M" + "," + j + "," + Mij);
					context.write(map_key, map_value);
				}
			}

			else if (fileName.contains("N")) {
				String[] tuple = value.toString().split(",");
				int j = Integer.parseInt(tuple[0]);
				//String[] tuples = tuple[1].split("\t");
				int k = Integer.parseInt(tuple[1]);
				int Njk = Integer.parseInt(tuple[2]);

				for (int i = 1; i < rowM + 1; i++) {
					map_key.set(i + "," + k);
					map_value.set("N" + "," + j + "," + Njk);
					context.write(map_key, map_value);
				}
			}
		}

	}

	// Reducer class
	public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
		private int sum = 0;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			columnM = Integer.parseInt(conf.get("columnM"));
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int[] M = new int[columnM + 1];
			int[] N = new int[columnM + 1];

			for (Text val : values) {
				String[] tuple = val.toString().split(",");
				if (tuple[0].equals("M")) {
					M[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
				} else
					N[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
			}

			/** 根据j值，对M[j]和N[j]进行相乘累加得到乘积矩阵的数据 **/
			for (int j = 1; j < columnM + 1; j++) {
				sum += M[j] * N[j];
			}
			context.write(key, new Text(Integer.toString(sum)));
			sum = 0;
		}
	}

	// Driver
	public int run(String[] args) throws Exception {

		String[] infoTupleM = args[0].split("_");
		rowM = Integer.parseInt(infoTupleM[1]);
		columnM = Integer.parseInt(infoTupleM[2]);
		String[] infoTupleN = args[1].split("_");
		columnN = Integer.parseInt(infoTupleN[2]);
		System.out.println(rowM + "\t" + columnM + "\t" + columnN);
		// set conf
		Configuration conf = super.getConf();

		/** 设置三个全局共享变量 **/
		conf.setInt("rowM", rowM);
		conf.setInt("columnM", columnM);
		conf.setInt("columnN", columnN);
		
		// create job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());

		// set job

		// 1) set class jar
		job.setJarByClass(this.getClass());

		// 2) set input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat
				.setInputPaths(job, new Path(args[0]), new Path(args[1]));

		// 3) set mapper class
		job.setMapperClass(MatrixMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 4) set shuffle
		// job.setPartitionerClass(HashPartitioner.class);
		// job.setSortComparatorClass(LongWritable.Comparator.class);
		// job.setCombinerClass(ModuleReducer.class);
		// job.setGroupingComparatorClass(LongWritable.Comparator.class);

		// 5) set reducer
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 6) set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// submit job
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://10.1.16.251:8020/user/hyman/mr/matrix/input/M_30_20",
				"hdfs://10.1.16.251:8020/user/hyman/mr/matrix/input/N_20_40",
				"hdfs://10.1.16.251:8020/user/hyman/mr/matrix/output" };

		// run job
		int status = ToolRunner.run(new MatrixMultiply(), args);

		// exit program
		System.exit(status);
	}

}
