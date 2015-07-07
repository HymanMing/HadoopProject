package cn.hduhadoop.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends
		RecordReader<NullWritable, BytesWritable> {
	private FileSplit fileSplit;
	private Configuration conf;

	private FSDataInputStream inStream = null;

	private BytesWritable value = new BytesWritable();

	private boolean processed = false;

	public WholeRecordReader() {
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if (!processed) {
			int length = (int) fileSplit.getLength();

			byte[] contents = new byte[length];

			Path path = fileSplit.getPath();

			FileSystem fs = path.getFileSystem(conf);

			inStream = fs.open(path);

			IOUtils.readFully(inStream, contents, 0, length);

			value.set(contents, 0, length);

			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeStream(inStream);
	}

}
