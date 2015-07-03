package cn.hduhadoop.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairWritable implements WritableComparable<PairWritable> {
	private int id;
	private String name;
	public PairWritable() {
	}
	public PairWritable(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);
		out.writeUTF(this.name);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.name = in.readUTF(); 
	}

	public int compareTo(PairWritable o) {
		int comp = this.id - o.getId();
		if (0 != comp)
			return comp;
		return this.name.compareTo(o.getName());
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		PairWritable other = (PairWritable) obj;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return id + "\t" + name;
	}
	
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(PairWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int comp = WritableComparator.compareBytes(b1, 0, 4, b2, 0, 4);
			if (0 != comp)
				return comp;
			
			return WritableComparator.compareBytes(b1, 4, l1-4, b2, 4, l2-4);
		}
		
	}
	
	static {
		WritableComparator.define(PairWritable.class, new Comparator());
	}
	
}
