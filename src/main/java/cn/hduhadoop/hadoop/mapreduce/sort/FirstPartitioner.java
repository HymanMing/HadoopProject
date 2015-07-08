package cn.hduhadoop.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区比较器
 * @author GUI
 *
 */
public class FirstPartitioner extends Partitioner<IntPairWritable, IntWritable> {

	@Override
	public int getPartition(IntPairWritable key, IntWritable value,
			int numPartitions) {
		return Math.abs((Integer.valueOf(key.getFirst()) * 127)) & numPartitions ;
	}

}
