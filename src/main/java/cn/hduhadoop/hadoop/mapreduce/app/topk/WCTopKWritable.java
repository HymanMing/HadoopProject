package cn.hduhadoop.hadoop.mapreduce.app.topk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WCTopKWritable implements WritableComparable<WCTopKWritable> {
	private String word;
	private long count;
	public WCTopKWritable() {
	}
	
	public WCTopKWritable(String word, long count) {
		set(word, count);
	}
	
	public void set(String word, long count) {
		setWord(word);
		setCount(count);
	}
	
	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeLong(count);
	}

	public void readFields(DataInput in) throws IOException {
		this.word = in.readUTF();
		this.count = in.readLong();
	}

	public int compareTo(WCTopKWritable o) {
		return Long.valueOf(this.getCount()).compareTo(o.getCount());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		WCTopKWritable other = (WCTopKWritable) obj;
		if (count != other.count)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return word + "\t" + count;
	}

	
}
