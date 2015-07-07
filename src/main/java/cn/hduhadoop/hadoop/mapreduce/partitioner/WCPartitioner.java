package cn.hduhadoop.hadoop.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		String keyStartChar = key.toString().substring(0, 1);
		if (keyStartChar.matches("[a-z]")) {
			return 0 % numPartitions;
		} 
		if (keyStartChar.matches("[A-Z]")) {
			return 1 % numPartitions;
		} 
		if (keyStartChar.matches("[0-9]")) {
			return 2 % numPartitions;
		}  
		
		return 3 % numPartitions;
	}

}
