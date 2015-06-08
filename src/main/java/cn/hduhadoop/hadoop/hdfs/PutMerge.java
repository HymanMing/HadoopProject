package cn.hduhadoop.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 合并本地的小文件写入到HDFS中
 * @author GUI
 *
 */
public class PutMerge {

	public static void main(String[] args) throws Exception {
		args = new String[]{
				"C:\\Users\\GUI\\Desktop\\input",
				"hdfs://10.1.16.251:8020/user/hyman/mr/input/mergefile.txt"
		};
		Configuration conf = new Configuration();
		
		FileSystem hdfs = FileSystem.get(conf);	// get HDFS FileSystem
		FileSystem local = FileSystem.getLocal(conf); // get Local FileSystem
		
		Path inputDir = new Path(args[0]); //set input path
		Path hdfsFile = new Path(args[1]); // set output file name
		
		FileStatus[] inputFiles = local.listStatus(inputDir); // 得到本地文件列表
		FSDataOutputStream out = hdfs.create(hdfsFile); // 生成HDFS输出流
		
		try {
			for (int i = 0; i < inputFiles.length; i++)
			{
				System.out.println("-------------" + inputFiles[i].getPath().getName() + "---------------");
				FSDataInputStream in = local.open(inputFiles[i].getPath()); // 打开本地输入流
				byte buffer[] = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
