package cn.hduhadoop.hadoop.hdfs;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsFsTest {

	public static void main(String[] args) throws Exception {
//		read();
//		readFromLocal();
//		write();
		write2();
	}
	
	/**
	 * read from HDFS
	 * @throws Exception
	 */
	public static void read() throws Exception {
		String fileUri = "/user/hyman/mr/input/wordcount";
		

		FileSystem fileSystem = HdfsUtils.getFileSystem();
		
		FSDataInputStream inStream = fileSystem.open(new Path(fileUri));
		
		try {
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			
		} finally {
			IOUtils.closeStream(inStream);
		}
	}
	
	/**
	 * read from LocalFileSystem
	 * @throws Exception
	 */
	public static void readFromLocal() throws Exception {

		String fileUri = "C:\\Users\\GUI\\Desktop\\wordcount";


		FileSystem fileSystem = HdfsUtils.getLocalFileSystem();
		
		FSDataInputStream inStream = fileSystem.open(new Path(fileUri));
		
		try {
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			
		} finally {
			IOUtils.closeStream(inStream);
		}
	}
	
	/**
	 * write to HDFS
	 * @throws Exception
	 */
	public static void write() throws Exception {
		FileSystem fileSystem = HdfsUtils.getFileSystem();
		FSDataOutputStream outStream = fileSystem.create(new Path("/user/hyman/mr/input/w_wordcount"));
		FileInputStream instream = new FileInputStream(new File("C:\\Users\\GUI\\Desktop\\wordcount"));
		
		try {
			IOUtils.copyBytes(instream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(instream);
			IOUtils.closeStream(outStream);
		}
	}
	
	/**
	 * write to HDFS from LocalFileSystem
	 * @throws Exception
	 */
	public static void write2() throws Exception {
		FileSystem fileSystem = HdfsUtils.getFileSystem();
		FSDataOutputStream outStream = fileSystem.create(new Path("/user/hyman/mr/input/put_w_wordcount"));
		InputStream instream = null;
		
		LocalFileSystem localFS = HdfsUtils.getLocalFileSystem();
		instream = localFS.open(new Path("C:\\Users\\GUI\\Desktop\\wordcount"));
		
		try {
			IOUtils.copyBytes(instream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(instream);
			IOUtils.closeStream(outStream);
		}
	}

}
