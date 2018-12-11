package org.gyt.lib1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @FileName: PutFile.java
 * @Package: org.gyt.lib1
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2018年11月1日 下午4:28:16
 */

public class PutFile {
	public static void main(String[] args) throws IOException, URISyntaxException {
		// 创建连接
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://slave2:9000"), new Configuration());
		// 本地文件路径
		Path src = new Path("/usr/local/java/tmp/testfile");
		// HDFS文件路径
		Path dst = new Path("/");
		// 复制文件
		fileSystem.copyFromLocalFile(src, dst);
		// 查询HDFS文件
		FileStatus[] fileStatus = fileSystem.listStatus(dst);
		for (FileStatus fileStatus2 : fileStatus) {
			System.out.println(fileStatus2.getPath());
		}
		fileSystem.close();
	}
}
