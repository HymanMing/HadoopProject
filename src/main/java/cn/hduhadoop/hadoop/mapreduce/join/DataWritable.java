package cn.hduhadoop.hadoop.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 自定义数据类型
 * 包含两个字段
 * Tag：表示表中信息的类型属于用户还是订单
 * data:表中的数据
 * @author GUI
 *
 */
public class DataWritable implements Writable {
	private String Tag;
	private String data;
	
	public DataWritable() {
	}
	
	public DataWritable(String Tag, String data) {
		set(Tag, data);
	}
	
	public void set(String Tag, String data) {
		setTag(Tag);
		setData(data);
	}
	
	public String getTag() {
		return Tag;
	}

	public void setTag(String tag) {
		Tag = tag;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.Tag);
		out.writeUTF(this.data);
	}

	public void readFields(DataInput in) throws IOException {
		this.Tag = in.readUTF();
		this.data = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((Tag == null) ? 0 : Tag.hashCode());
		result = prime * result + ((data == null) ? 0 : data.hashCode());
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
		DataWritable other = (DataWritable) obj;
		if (Tag == null) {
			if (other.Tag != null)
				return false;
		} else if (!Tag.equals(other.Tag))
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return Tag + "," + data;
	}
	
	

}
