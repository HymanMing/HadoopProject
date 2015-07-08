package cn.hduhadoop.hadoop.mapreduce.sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator implements RawComparator<IntPairWritable> {

	public int compare(IntPairWritable o1, IntPairWritable o2) {
		return Integer.valueOf(o1.getFirst()).compareTo(o2.getFirst());
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
	}

}
