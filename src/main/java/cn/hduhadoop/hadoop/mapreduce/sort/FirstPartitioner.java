package cn.hduhadoop.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<IntPairWritable, IntWritable> {

	@Override
	public int getPartition(IntPairWritable key, IntWritable value,
			int numPartitions) {
		return Math.abs((Integer.valueOf(key.getFirst()) * 127)) & numPartitions ;
	}

}
