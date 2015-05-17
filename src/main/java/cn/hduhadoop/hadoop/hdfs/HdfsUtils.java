package cn.hduhadoop.hadoop.hdfs;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

public class HdfsUtils {
	
	/**
	 * get LocalFileSystem
	 * @return LocalFileSystem
	 * @throws Exception
	 */
	public static LocalFileSystem getLocalFileSystem() throws Exception {
		Configuration conf = new Configuration();
		
		LocalFileSystem fileSystem = FileSystem.getLocal(conf);
		
		return fileSystem;
	}
	
	/**
	 * default method about getFileSystem(the FileSystem is HDFS)
	 * @return HDFS FileSystem
	 * @throws Exception
	 */
	public static FileSystem getFileSystem() throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(conf);
		
		return fileSystem;
	}
	
	
	/**
	 * get FileSystem appoint the HDFS user(the FileSystem is HDFS)
	 * @param user
	 * @return HDFS FileSystem
	 * @throws Exception
	 */
	public static FileSystem getFileSystemUser(String user) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set(//
				"fs.defaultFS", //
				"hdfs://server-603:8020");
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://server-603:8020"), conf, user);
		
		return fileSystem;
	}
}
