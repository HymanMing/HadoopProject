package cn.hduhadoop.hadoop.hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class HdfsUrlTest {

	// 注册  让Java程序识别hdfs URL形式
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		String fileUrl = //
				"hdfs://server-603:8020"//
				+ "/user/hyman/mr/input/wordcount";
		
		InputStream inStream = null;
		
		try {
			inStream = new URL(fileUrl).openStream();
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		}catch(Exception e)
		{
			e.printStackTrace();
			
		}finally {
			IOUtils.closeStream(inStream);
		}
	}

}
