package org.gyt.lib1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @FileName: GetFile.java
 * @Package: org.gyt.lib1
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月1日 下午4:43:39
 */

public class GetFile {
	public static void main(String[] args) throws IOException, URISyntaxException {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://slave2:9000"), new Configuration());
		// HDFS path
		Path src = new Path("/testfile");
		// local path
		Path dst = new Path("/home/hadoop/HDFS");
		fileSystem.copyToLocalFile(src, dst);
		fileSystem.close();
	}
}
