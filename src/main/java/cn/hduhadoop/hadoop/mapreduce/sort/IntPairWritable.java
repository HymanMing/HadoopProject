package cn.hduhadoop.hadoop.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairWritable implements WritableComparable<IntPairWritable> {
	private int first;
	private int second;
	
	public IntPairWritable() {}
	
	public IntPairWritable(int first, int second) {
		set(first, second);
	}
	
	public void set(int first, int second) {
		setFirst(first);
		setSecond(second);
	}
	
	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(getFirst());
		out.writeInt(getSecond());
	}

	public void readFields(DataInput in) throws IOException {
		this.first = in.readInt();
		this.second = in.readInt();
	}

	public int compareTo(IntPairWritable o) {
		int comp = Integer.valueOf(this.getFirst()).compareTo(o.getFirst());
		if (0 != comp) {
			return comp;
		}
		return Integer.valueOf(this.getSecond()).compareTo(o.getSecond());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntPairWritable other = (IntPairWritable) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}
	
	static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntPairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return super.compareBytes(b1, s1, l1, b2, s2, l2);
		}
		
	}
	
	static {
		WritableComparator.define(IntPairWritable.class, new Comparator());
	}
	
}
