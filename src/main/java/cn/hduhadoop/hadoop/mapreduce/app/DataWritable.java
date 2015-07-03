package cn.hduhadoop.hadoop.mapreduce.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable {
	
	private long upPackNum;	// 上行数据包数
	private long downPackNum;	// 下行数据包数
	private long upPayLoad;	// 上行总流量
	private long downPayLoad;	// 下行总流量
	
	public DataWritable() {
	}
	
	public DataWritable(long upPackNum, long downPackNum, long upPayLoad, long downPayLoad) {
		set(upPackNum, downPackNum, upPayLoad, downPayLoad);
	}
	
	public long getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(long upPackNum) {
		this.upPackNum = upPackNum;
	}

	public long getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(long downPackNum) {
		this.downPackNum = downPackNum;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public void set(long upPackNum, long downPackNum, long upPayLoad, long downPayLoad) {
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}

	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}

	@Override
	public String toString() {
		return upPackNum + "\t"
				+ downPackNum + "\t" + upPayLoad + "\t"
				+ downPayLoad;
	}

	
}
